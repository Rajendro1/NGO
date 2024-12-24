func (psr *PlanSubmnRqmtProcessor) Process(packageDir string) (bool, error) {
	if err := psr.repository.CreateTempTableClaimParms(psr.ctx, psr.tx); err != nil {
		return false, err
	}

	if err := psr.repository.DeleteTempTableClaimParms(psr.ctx, psr.tx); err != nil {
		return false, err
	}

	unlFile := fmt.Sprintf(constants.UnlFileClaimParms, psr.basePath)
	psr.logger.Info("Loading unl file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, psr.fileMetadata.FileId)
	fileData, err := psr.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		psr.logger.Error("Error loading the unl file", constants.LoggingKeyUnlFileName, unlFile)
		return false, err
	}

	for _, row := range fileData {
		for index, value := range row {
			if index == 0 {
				claimParmNbr, _ := strconv.Atoi(value)
				if err = psr.repository.InsertClaimParm(psr.ctx, psr.tx, claimParmNbr); err != nil {
					psr.logger.Error("Error inserting claim parm", "claimParmNbr", claimParmNbr)
					return false, err
				}
			}
		}
	}

	isFirstTime := true

	unlFile = fmt.Sprintf(constants.UnlFilePlanSubmnRqmt, packageDir, psr.fileMetadata.FileId)

	psr.logger.Info("Loading unl file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, psr.fileMetadata.FileId)
	fileData, err = psr.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		psr.logger.Error("Error loading the unl file", constants.LoggingKeyUnlFileName, unlFile)
		return false, err
	}

	for _, row := range fileData {
		planSubmnRqmt, err := NewPlanSubmnRqmt(row)
		if err != nil {
			psr.logger.Error("Error creating plan submn rqmt", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
			return false, err
		}

		var count int
		if count, err = psr.repository.GetPlanCount(psr.ctx, psr.tx, planSubmnRqmt.CarrierCoId, planSubmnRqmt.InsCarrPlanId); err != nil {
			psr.logger.Error("Error getting plan count", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
			return false, err
		}
		if count <= 0 {
			return true, fmt.Errorf("skipping planSubmnRqmt record processing as plan does not exist with CarrierCoId: %d and InsCarrPlan %d", planSubmnRqmt.CarrierCoId, planSubmnRqmt.InsCarrPlanId)
		}
		if isFirstTime {
			if err := psr.repository.DeletePlanSubmn(psr.ctx, psr.tx, planSubmnRqmt.CarrierCoId, planSubmnRqmt.InsCarrPlanId); err != nil {
				psr.logger.Error("Error deleting plan submn rqmt", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
				return false, err
			}
			if err := psr.repository.DeleteD0PlanSubmn(psr.ctx, psr.tx, planSubmnRqmt.CarrierCoId, planSubmnRqmt.InsCarrPlanId); err != nil {
				psr.logger.Error("Error deleting D0 plan submn", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
				return false, err
			}
			isFirstTime = false
		}
		count, err = psr.repository.GetClaimParmCount(psr.ctx, psr.tx, planSubmnRqmt.ClaimParmNbr)
		if err != nil {
			psr.logger.Error("Error gettingclaim parm count", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
			return false, err
		}
		if count > 0 {
			if err = psr.repository.InsertPlanSubmnRqmt(psr.ctx, psr.tx, planSubmnRqmt); err != nil {
				psr.logger.Error("Error inserting plan submn rqmt", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
				return false, err
			}
		} else {
			if err = psr.repository.InsertD0PlanSubmnRqmt(psr.ctx, psr.tx, planSubmnRqmt); err != nil {
				psr.logger.Error("Error inserting D0 plan submn rqmt", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
				return false, err
			}
		}
	}
	return true, nil
}
