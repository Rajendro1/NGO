func (psr *PlanSubmnRqmtProcessor) Process(packageDir string) (bool, error) {
	if err := psr.prepareTempTable(); err != nil {
		return false, err
	}

	if err := psr.processClaimParmsFile(); err != nil {
		return false, err
	}
	isFirstTime := true

	if err := psr.processPlanSubmnRqmtFile(packageDir, &isFirstTime); err != nil {
		return false, err
	}

	return true, nil
}

func (psr *PlanSubmnRqmtProcessor) prepareTempTable() error {
	if err := psr.repository.CreateTempTableClaimParms(psr.ctx, psr.tx); err != nil {
		return err
	}

	if err := psr.repository.DeleteTempTableClaimParms(psr.ctx, psr.tx); err != nil {
		return err
	}

	return nil
}

func (psr *PlanSubmnRqmtProcessor) processClaimParmsFile() error {
	unlFile := fmt.Sprintf(constants.UnlFileClaimParms, psr.basePath)
	psr.logger.Info("Loading UNL file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, psr.fileMetadata.FileId)

	fileData, err := psr.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		psr.logger.Error("Error loading the UNL file", constants.LoggingKeyUnlFileName, unlFile)
		return err
	}

	for _, row := range fileData {
		if err := psr.insertClaimParms(row); err != nil {
			return err
		}
	}

	return nil
}

func (psr *PlanSubmnRqmtProcessor) insertClaimParms(row []string) error {
	for index, value := range row {
		if index == 0 {
			claimParmNbr, _ := strconv.Atoi(value)
			if err := psr.repository.InsertClaimParm(psr.ctx, psr.tx, claimParmNbr); err != nil {
				psr.logger.Error("Error inserting claim parm", "claimParmNbr", claimParmNbr)
				return err
			}
		}
	}
	return nil
}

func (psr *PlanSubmnRqmtProcessor) processPlanSubmnRqmtFile(packageDir string, isFirstTime *bool) error {

	unlFile := fmt.Sprintf(constants.UnlFilePlanSubmnRqmt, packageDir, psr.fileMetadata.FileId)
	psr.logger.Info("Loading UNL file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, psr.fileMetadata.FileId)

	fileData, err := psr.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		psr.logger.Error("Error loading the UNL file", constants.LoggingKeyUnlFileName, unlFile)
		return err
	}

	for _, row := range fileData {
		if err := psr.processPlanSubmnRqmtRow(row, isFirstTime); err != nil {
			return err
		}
	}

	return nil
}

func (psr *PlanSubmnRqmtProcessor) processPlanSubmnRqmtRow(row []string, isFirstTime *bool) error {
	planSubmnRqmt, err := NewPlanSubmnRqmt(row)
	if err != nil {
		psr.logger.Error("Error creating plan submn rqmt", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
		return err
	}

	if err := psr.validatePlanExistence(planSubmnRqmt); err != nil {
		return err
	}

	if *isFirstTime {
		if err := psr.deleteExistingPlanData(planSubmnRqmt); err != nil {
			return err
		}
		*isFirstTime = false
	}

	if err := psr.insertPlanData(planSubmnRqmt); err != nil {
		return err
	}

	return nil
}

func (psr *PlanSubmnRqmtProcessor) validatePlanExistence(planSubmnRqmt models.PlanSubmRqmt) error {
	count, err := psr.repository.GetPlanCount(psr.ctx, psr.tx, planSubmnRqmt.CarrierCoId, planSubmnRqmt.InsCarrPlanId)
	if err != nil {
		psr.logger.Error("Error getting plan count", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
		return err
	}
	if count <= 0 {
		return fmt.Errorf("skipping planSubmnRqmt record processing as plan does not exist with CarrierCoId: %d and InsCarrPlan %d", planSubmnRqmt.CarrierCoId, planSubmnRqmt.InsCarrPlanId)
	}
	return nil
}

func (psr *PlanSubmnRqmtProcessor) deleteExistingPlanData(planSubmnRqmt models.PlanSubmRqmt) error {
	if err := psr.repository.DeletePlanSubmn(psr.ctx, psr.tx, planSubmnRqmt.CarrierCoId, planSubmnRqmt.InsCarrPlanId); err != nil {
		psr.logger.Error("Error deleting plan submn rqmt", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
		return err
	}

	if err := psr.repository.DeleteD0PlanSubmn(psr.ctx, psr.tx, planSubmnRqmt.CarrierCoId, planSubmnRqmt.InsCarrPlanId); err != nil {
		psr.logger.Error("Error deleting D0 plan submn", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
		return err
	}

	return nil
}

func (psr *PlanSubmnRqmtProcessor) insertPlanData(planSubmnRqmt models.PlanSubmRqmt) error {
	count, err := psr.repository.GetClaimParmCount(psr.ctx, psr.tx, planSubmnRqmt.ClaimParmNbr)
	if err != nil {
		psr.logger.Error("Error getting claim parm count", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
		return err
	}

	if count > 0 {
		if err := psr.repository.InsertPlanSubmnRqmt(psr.ctx, psr.tx, planSubmnRqmt); err != nil {
			psr.logger.Error("Error inserting plan submn rqmt", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
			return err
		}
	} else {
		if err := psr.repository.InsertD0PlanSubmnRqmt(psr.ctx, psr.tx, planSubmnRqmt); err != nil {
			psr.logger.Error("Error inserting D0 plan submn rqmt", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
			return err
		}
	}

	return nil
}
