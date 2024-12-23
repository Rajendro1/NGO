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
	unlFile, err := cir.loadFile(packageDir)
	if err != nil {
		return false, err
	}

	fileData, err := cir.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		cir.logger.Error("Error loading the unl file", constants.LoggingKeyUnlFileName, unlFile)
		return false, err
	}

	if err := cir.processFileData(fileData); err != nil {
		return false, err
	}

	return true, nil
}

func (cir *CarrItemRestrictProcessor) loadFile(packageDir string) (string, error) {
	unlFile := fmt.Sprintf(constants.UnlFileCarrierItemRestrict, packageDir, cir.fileMetadata.FileId)
	cir.logger.Info("Loading unl file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, cir.fileMetadata.FileId)
	return unlFile, nil
}

func (cir *CarrItemRestrictProcessor) processFileData(fileData [][]string) error {
	var previousCarrItemRestrict models.CarrItemRestrict

	for _, row := range fileData {
		carrItemRestrict, err := NewCarrItemRestrict(row)
		if err != nil {
			return cir.handleError(constants.LoggingKeyCarrierCoId, carrItemRestrict, err)
		}

		if cir.isNewRestrict(previousCarrItemRestrict, carrItemRestrict) {
			previousCarrItemRestrict = carrItemRestrict
			if err := cir.handleNewRestrict(carrItemRestrict); err != nil {
				return err
			}
		}

		if err := cir.handlePlanAndItemCount(carrItemRestrict); err != nil {
			return err
		}
	}

	return nil
}

func (cir *CarrItemRestrictProcessor) handlePlanAndItemCount(carrItemRestrict models.CarrItemRestrict) error {
	if err := cir.handlePlanCount(carrItemRestrict); err != nil && !errors.Is(err, conutErr) {
		return err
	}

	if err := cir.handleItemCount(carrItemRestrict); err != nil && !errors.Is(err, conutErr) {
		return err
	}

	if cir.subjectArea != constants.SubjectAreaItemTpRmpkg {
		if err := cir.repository.InsertCarrItemRestrict(cir.ctx, cir.tx, carrItemRestrict); err != nil {
			return cir.handleError(constants.LoggingKeyInsCarrPlanId, carrItemRestrict, err)
		}
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
