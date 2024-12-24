package fileprocessorfactory

import (
	"context"
	"database/sql"
	"errors"
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

var conutErr = errors.New("invalid count error")

type CarrItemRestrictRepository interface {
	GetItemRestrictCount(ctx context.Context, tx *sql.Tx, carrierCoId, insCarrierPlanId, coverageOptionId, itemMdsFamId int) (int, error)
	DeleteCarrItemRestrict(ctx context.Context, tx *sql.Tx, carrierCoId, insCarrierPlanId, coverageOptionId, itemMdsFamId int) error
	GetPlanCount(ctx context.Context, tx *sql.Tx, carrierCoId, insCarrierPlanId int) (int, error)
	GetItemCount(ctx context.Context, tx *sql.Tx, itemMdsFamId int) (int, error)
	InsertCarrItemRestrict(ctx context.Context, tx *sql.Tx, carrItemRestrict models.CarrItemRestrict) error
}

func NewCarrItemRestrictProcessor(logger Logger, fileUtils utils.FileUtilMethods, fileMetadata *models.FileMetadata, repository CarrItemRestrictRepository, ctx context.Context, tx *sql.Tx, subjectArea int) *CarrItemRestrictProcessor {
	return &CarrItemRestrictProcessor{logger, fileUtils, fileMetadata, repository, ctx, tx, subjectArea}
}

func (cir *CarrItemRestrictProcessor) Process(packageDir string) (bool, error) {
	unlFile := fmt.Sprintf(constants.UnlFileCarrierItemRestrict, packageDir, cir.fileMetadata.FileId)
	cir.logger.Info("Loading unl file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, cir.fileMetadata.FileId)

	fileData, err := cir.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		cir.logger.Error("Error loading the unl file", constants.LoggingKeyUnlFileName, unlFile)
		return false, err
	}

	return cir.processFileData(fileData)
}

func (cir *CarrItemRestrictProcessor) processFileData(fileData [][]string) (bool, error) {
	var previousCarrItemRestrict models.CarrItemRestrict

	for _, row := range fileData {
		carrItemRestrict, err := NewCarrItemRestrict(row)
		if err != nil {
			return false, cir.handleError(constants.LoggingKeyCarrierCoId, carrItemRestrict, err)
		}

		if err := cir.processRestrict(carrItemRestrict, &previousCarrItemRestrict); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (cir *CarrItemRestrictProcessor) processRestrict(carrItemRestrict models.CarrItemRestrict, previous *models.CarrItemRestrict) error {
	if cir.isNewRestrict(*previous, carrItemRestrict) {
		*previous = carrItemRestrict
		if err := cir.handleNewRestrict(carrItemRestrict); err != nil {
			return err
		}
	}

	if err := cir.handlePlanAndItemCount(carrItemRestrict); err != nil {
		return err
	}

	return nil
}

func (cir *CarrItemRestrictProcessor) isNewRestrict(previous, current models.CarrItemRestrict) bool {
	return current.CarrierCoId != previous.CarrierCoId || current.InsCarrPlanId != previous.InsCarrPlanId || current.CoverageOptionId != previous.CoverageOptionId || current.ItemMdsFamId != previous.ItemMdsFamId
}

func (cir *CarrItemRestrictProcessor) handleError(key string, carrItemRestrict models.CarrItemRestrict, err error) (bool, error) {
	cir.logger.Error("Error", key, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId)
	return false, err
}

func (cir *CarrItemRestrictProcessor) handleNewRestrict(carrItemRestrict models.CarrItemRestrict) error {
	count, err := cir.repository.GetItemRestrictCount(cir.ctx, cir.tx, carrItemRestrict.CarrierCoId, carrItemRestrict.InsCarrPlanId, carrItemRestrict.CoverageOptionId, carrItemRestrict.ItemMdsFamId)
	if err != nil {
		return err
	}
	if count > 0 {
		return cir.repository.DeleteCarrItemRestrict(cir.ctx, cir.tx, carrItemRestrict.CarrierCoId, carrItemRestrict.InsCarrPlanId, carrItemRestrict.CoverageOptionId, carrItemRestrict.ItemMdsFamId)
	}
	return nil
}

func (cir *CarrItemRestrictProcessor) handlePlanCount(carrItemRestrict models.CarrItemRestrict) error {
	planCount, err := cir.repository.GetPlanCount(cir.ctx, cir.tx, carrItemRestrict.CarrierCoId, carrItemRestrict.InsCarrPlanId)
	if err != nil {
		return err
	}
	if planCount <= 0 {
		cir.logger.Warn("Plan does not exist, hence continuing with next item restrict", constants.LoggingKeyCarrierCoId, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId)
		return conutErr
	}
	return nil
}

func (cir *CarrItemRestrictProcessor) handleItemCount(carrItemRestrict models.CarrItemRestrict) error {
	itemCount, err := cir.repository.GetItemCount(cir.ctx, cir.tx, carrItemRestrict.ItemMdsFamId)
	if err != nil {
		return err
	}
	if itemCount <= 0 {
		cir.logger.Warn("Item does not exist, hence continuing with next item restrict", constants.LoggingKeyItemMdsFamId, carrItemRestrict.ItemMdsFamId)
		return conutErr
	}
	return nil
}
func NewCarrItemRestrict(row []string) (models.CarrItemRestrict, error) {
	carrItemRestrict := models.CarrItemRestrict{}
	var err error

	fields := []func(string) error{
		func(v string) error { return utils.StringToInt(v, "CarrierCoId", &carrItemRestrict.CarrierCoId) },
		func(v string) error { return utils.StringToInt(v, "InsCarrPlanId", &carrItemRestrict.InsCarrPlanId) },
		func(v string) error {
			return utils.StringToInt(v, "CoverageOptionId", &carrItemRestrict.CoverageOptionId)
		},
		func(v string) error { return utils.StringToInt(v, "ItemMdsFamId", &carrItemRestrict.ItemMdsFamId) },
		func(v string) error { carrItemRestrict.RestrictTypeCode, err = strconv.Atoi(v); return err },
		func(v string) error { carrItemRestrict.Restrictionvalue = v; return nil },
	}

	for i, value := range row {
		if err := fields[i](value); err != nil {
			return carrItemRestrict, fmt.Errorf("error processing field %d: %v", i, err)
		}
	}
	return carrItemRestrict, nil
}
