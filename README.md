package fileprocessorfactory

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"gecgithub01.walmart.com/CustomerTechnologies/insurancePlanApply/internal/constants"
	"gecgithub01.walmart.com/CustomerTechnologies/insurancePlanApply/internal/models"
	"gecgithub01.walmart.com/CustomerTechnologies/insurancePlanApply/internal/utils"
)

type CarrItemRestrictProcessor struct {
	logger       Logger
	fileUtils    utils.FileUtilMethods
	fileMetadata *models.FileMetadata
	repository   CarrItemRestrictRepository
	ctx          context.Context
	tx           *sql.Tx
	subjectArea  int
}

type CarrItemRestrictRepository interface {
	GetItemRestrictCount(ctx context.Context, tx *sql.Tx, carrierCoId int, insCarrierPlanId int, coverageOptionId int, itemMdsFamId int) (int, error)
	DeleteCarrItemRestrict(ctx context.Context, tx *sql.Tx, carrierCoId int, insCarrierPlanId int, coverageOptionId int, itemMdsFamId int) error
	GetPlanCount(ctx context.Context, tx *sql.Tx, carrierCoId int, insCarrierPlanId int) (int, error)
	GetItemCount(ctx context.Context, tx *sql.Tx, itemMdsFamId int) (int, error)
	InsertCarrItemRestrict(ctx context.Context, tx *sql.Tx, carrItemRestrict models.CarrItemRestrict) error
}

func NewCarrItemRestrictProcessor(logger Logger, fileUtils utils.FileUtilMethods, fileMetadata *models.FileMetadata, repository CarrItemRestrictRepository, ctx context.Context, tx *sql.Tx, subjectArea int) *CarrItemRestrictProcessor {
	return &CarrItemRestrictProcessor{logger: logger, fileUtils: fileUtils, fileMetadata: fileMetadata, repository: repository, ctx: ctx, tx: tx, subjectArea: subjectArea}
}

func (cir *CarrItemRestrictProcessor) Process(packageDir string) (bool, error) {
	unlFile := fmt.Sprintf(constants.UnlFileCarrierItemRestrict, packageDir, cir.fileMetadata.FileId)
	cir.logger.Info("Loading unl file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, cir.fileMetadata.FileId)

	fileData, err := cir.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		cir.logger.Error("Error loading the unl file", constants.LoggingKeyUnlFileName, unlFile)
		return false, err
	}

	for _, row := range fileData {
		if success, err := cir.processFileData(row); !success {
			return false, err // Logs are handled in processFileData
		}
	}
	return true, nil
}

func (cir *CarrItemRestrictProcessor) processFileData(row []string) (bool, error) {
	carrItemRestrict, err := NewCarrItemRestrict(row)
	if err != nil {
		cir.logger.Error("Error creating carr item restrict object", constants.LoggingKeyCarrierCoId, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId)
		return false, err
	}
	// var (
	// 	previousCarrierCoId      int
	// 	previousInsCarrPlanId    int
	// 	previousCoverageOptionId int
	// 	previousItemMdsFamId     int
	// 	err                      error
	// )
	// if carrItemRestrict.CarrierCoId != previousCarrierCoId || carrItemRestrict.InsCarrPlanId != previousInsCarrPlanId || carrItemRestrict.CoverageOptionId != previousCoverageOptionId || carrItemRestrict.ItemMdsFamId != previousItemMdsFamId {
	// 	previousCarrierCoId = carrItemRestrict.CarrierCoId
	// 	previousInsCarrPlanId = carrItemRestrict.InsCarrPlanId
	// 	previousCoverageOptionId = carrItemRestrict.CoverageOptionId
	// 	previousItemMdsFamId = carrItemRestrict.ItemMdsFamId
	var previousCarrItemRestrict *models.PreviousCarrItemRestrict
	if carrItemRestrict.CarrierCoId != previousCarrItemRestrict.PreviousCarrierCoId || carrItemRestrict.InsCarrPlanId != previousCarrItemRestrict.PreviousInsCarrPlanId || carrItemRestrict.CoverageOptionId != previousCarrItemRestrict.PreviousCoverageOptionId || carrItemRestrict.ItemMdsFamId != previousCarrItemRestrict.PreviousItemMdsFamId {
		previousCarrItemRestrict.PreviousCarrierCoId = carrItemRestrict.CarrierCoId
		previousCarrItemRestrict.PreviousInsCarrPlanId = carrItemRestrict.InsCarrPlanId
		previousCarrItemRestrict.PreviousCoverageOptionId = carrItemRestrict.CoverageOptionId
		previousCarrItemRestrict.PreviousItemMdsFamId = carrItemRestrict.ItemMdsFamId

		if err := cir.handleItemRestriction(carrItemRestrict); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (cir *CarrItemRestrictProcessor) handleItemRestriction(carrItemRestrict models.CarrItemRestrict) error {
	if err := cir.validateAndDeleteExistingRestrictions(carrItemRestrict); err != nil {
		return err
	}
	return cir.insertOrGetItemRestriction(carrItemRestrict)
}

func (cir *CarrItemRestrictProcessor) validateAndDeleteExistingRestrictions(carrItemRestrict models.CarrItemRestrict) error {
	count, err := cir.repository.GetItemRestrictCount(cir.ctx, cir.tx, carrItemRestrict.CarrierCoId, carrItemRestrict.InsCarrPlanId, carrItemRestrict.CoverageOptionId, carrItemRestrict.ItemMdsFamId)
	if err != nil {
		cir.logger.Error("Error getting item restrict count", constants.LoggingKeyCarrierCoId, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId, constants.LoggingKeyCoverageOptionId, carrItemRestrict.CoverageOptionId, constants.LoggingKeyItemMdsFamId, carrItemRestrict.ItemMdsFamId)
		return err
	}

	if count > 0 {
		if err = cir.repository.DeleteCarrItemRestrict(cir.ctx, cir.tx, carrItemRestrict.CarrierCoId, carrItemRestrict.InsCarrPlanId, carrItemRestrict.CoverageOptionId, carrItemRestrict.ItemMdsFamId); err != nil {
			cir.logger.Error("Error deleting carr item restrict", constants.LoggingKeyCarrierCoId, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId, constants.LoggingKeyCoverageOptionId, carrItemRestrict.CoverageOptionId, constants.LoggingKeyItemMdsFamId, carrItemRestrict.ItemMdsFamId)
			return err
		}
	}
	return nil
}

func (cir *CarrItemRestrictProcessor) insertOrGetItemRestriction(carrItemRestrict models.CarrItemRestrict) error {
	planCount, err := cir.repository.GetPlanCount(cir.ctx, cir.tx, carrItemRestrict.CarrierCoId, carrItemRestrict.InsCarrPlanId)
	if err != nil {
		cir.logger.Error("Error getting plan count", constants.LoggingKeyCarrierCoId, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId)
		return err
	}
	if planCount <= 0 {
		cir.logger.Warn("Plan does not exist, hence skipping the insert/update for this item restrict", constants.LoggingKeyCarrierCoId, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId)
		return nil
	}

	itemCount, err := cir.repository.GetItemCount(cir.ctx, cir.tx, carrItemRestrict.ItemMdsFamId)
	if err != nil {
		cir.logger.Error("Error getting item count", constants.LoggingKeyItemMdsFamId, carrItemRestrict.ItemMdsFamId)
		return err
	}
	if itemCount <= 0 {
		cir.logger.Warn("Item does not exist, hence skipping the insert/update for this item restrict", constants.LoggingKeyItemMdsFamId, carrItemRestrict.ItemMdsFamId)
		return nil
	}

	if cir.subjectArea != constants.SubjectAreaItemTpRmpkg {
		if err := cir.repository.InsertCarrItemRestrict(cir.ctx, cir.tx, carrItemRestrict); err != nil {
			cir.logger.Error("Error inserting carr item restrict", constants.LoggingKeyCarrierCoId, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId)
			return err
		}
	}
	return nil
}

func NewCarrItemRestrict(row []string) (models.CarrItemRestrict, error) {
	var carrItemRestrict models.CarrItemRestrict
	var err error
	for index, value := range row {
		switch index {
		case 0:
			carrItemRestrict.CarrierCoId, err = utils.StringToInt(value, "carrPlanParm.CarrierCoId", err)
		case 1:
			carrItemRestrict.InsCarrPlanId, err = utils.StringToInt(value, "carrPlanParm.InsCarrPlanId", err)
		case 2:
			carrItemRestrict.CoverageOptionId, err = utils.StringToInt(value, "carrPlanParm.CoverageOptionId", err)
		case 3:
			carrItemRestrict.ItemMdsFamId, err = utils.StringToInt(value, "carrPlanParm.MdsFamId", err)
		case 4:
			carrItemRestrict.RestrictTypeCode, err = strconv.Atoi(value)
		case 5:
			carrItemRestrict.Restrictionvalue = value
		}
		if err != nil {
			return carrItemRestrict, fmt.Errorf("error converting string to int for index %d: %v", index, err)
		}
	}
	return carrItemRestrict, nil
}
