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

var (
	conutErr = errors.New("invalid count error")
)

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

	var previousCarrItemRestrict models.CarrItemRestrict

	for _, row := range fileData {
		carrItemRestrict, err := NewCarrItemRestrict(row)
		if err != nil {
			return cir.handleError(constants.LoggingKeyCarrierCoId, carrItemRestrict, err)
		}

		if cir.isNewRestrict(previousCarrItemRestrict, carrItemRestrict) {
			previousCarrItemRestrict = carrItemRestrict
			if err := cir.handleNewRestrict(carrItemRestrict); err != nil {
				return false, err
			}
		}

		if err := cir.handlePlanCount(carrItemRestrict); err != nil {
			if errors.Is(err, conutErr) {
				continue
			}
			return false, err
		}

		if err := cir.handleItemCount(carrItemRestrict); err != nil {
			if errors.Is(err, conutErr) {
				continue
			}
			return false, err
		}

		if cir.subjectArea != constants.SubjectAreaItemTpRmpkg {
			if err := cir.repository.InsertCarrItemRestrict(cir.ctx, cir.tx, carrItemRestrict); err != nil {
				return cir.handleError(constants.LoggingKeyInsCarrPlanId, carrItemRestrict, err)
			}
		}
	}
	return true, nil
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
	var (
		carrItemRestrict models.CarrItemRestrict
		err              error
	)
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
			carrItemRestrict.RestrictTypeCode, _ = strconv.Atoi(value)
		case 5:
			carrItemRestrict.Restrictionvalue = value
		}
	}
	if err != nil {
		return carrItemRestrict, err
	}
	return carrItemRestrict, nil
}
