func (psr *PlanSubmnRqmtProcessor) Process(packageDir string) (bool, error) {
    if err := psr.setupTempTable(); err != nil {
        return false, err
    }

    if err := psr.processClaimParms(); err != nil {
        return false, err
    }

    return psr.processPlanSubmnRqmts(packageDir)
}

func (psr *PlanSubmnRqmtProcessor) setupTempTable() error {
    if err := psr.repository.CreateTempTableClaimParms(psr.ctx, psr.tx); err != nil {
        return err
    }
    return psr.repository.DeleteTempTableClaimParms(psr.ctx, psr.tx)
}

func (psr *PlanSubmnRqmtProcessor) processClaimParms() error {
    unlFile := fmt.Sprintf(constants.UnlFileClaimParms, psr.basePath)
    psr.logger.Info("Loading unl file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, psr.fileMetadata.FileId)
    fileData, err := psr.fileUtils.ReadUnlFiledata(unlFile)
    if err != nil {
        psr.logger.Error("Error loading the unl file", constants.LoggingKeyUnlFileName, unlFile)
        return err
    }

    for _, row := range fileData {
        if claimParmNbr, err := strconv.Atoi(row[0]); err == nil {
            if err := psr.repository.InsertClaimParm(psr.ctx, psr.tx, claimParmNbr); err != nil {
                psr.logger.Error("Error inserting claim parm", "claimParmNbr", claimParmNbr)
                return err
            }
        }
    }
    return nil
}

func (psr *PlanSubmnRqmtProcessor) processPlanSubmnRqmts(packageDir string) (bool, error) {
    unlFile := fmt.Sprintf(constants.UnlFilePlanSubmnRqmt, packageDir, psr.fileMetadata.FileId)
    psr.logger.Info("Loading unl file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, psr.fileMetadata.FileId)
    fileData, err := psr.fileUtils.ReadUnlFiledata(unlFile)
    if err != nil {
        psr.logger.Error("Error loading the unl file", constants.LoggingKeyUnlFileName, unlFile)
        return false, err
    }

    for _, row := range fileData {
        if err := psr.processSinglePlanSubmnRqmt(row); err != nil {
            return false, err
        }
    }
    return true, nil
}

func (psr *PlanSubmnRqmtProcessor) processSinglePlanSubmnRqmt(row []string) error {
    planSubmnRqmt, err := NewPlanSubmnRqmt(row)
    if err != nil {
        psr.logger.Error("Error creating plan submn rqmt", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
        return err
    }

    if err := psr.checkAndProcessPlanSubmn(planSubmnRqmt); err != nil {
        return err
    }

    return psr.handleClaimParms(planSubmnRqmt)
}

func (psr *PlanSubmnRqmtProcessor) checkAndProcessPlanSubmn(planSubmnRqmt *PlanSubmnRqmt) error {
    count, err := psr.repository.GetPlanCount(psr.ctx, psr.tx, planSubmnRqmt.CarrierCoId, planSubmnRqmt.InsCarrPlanId)
    if err != nil {
        psr.logger.Error("Error getting plan count", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
        return err
    }
    if count <= 0 {
        return fmt.Errorf("skipping planSubmnRqmt record processing as plan does not exist with CarrierCoId: %d and InsCarrPlanId: %d", planSubmnRqmt.CarrierCoId, planSubmnRqmt.InsCarrPlanId)
    }

    return psr.cleanUpPlans(planSubmnRqmt)
}

func (psr *PlanSubmnRqmtProcessor) cleanUpPlans(planSubmnRqmt *PlanSubmnRqmt) error {
    if err := psr.repository.DeletePlanSubmn(psr.ctx, psr.tx, planSubmnRqmt.CarrierCoId, planSubmnRqmt.InsCarrPlanId); err != nil {
        psr.logger.Error("Error deleting plan submn rqmt", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
        return err
    }
    return psr.repository.DeleteD0PlanSubmn(psr.ctx, psr.tx, planSubmnRqmt.CarrierCoId, planSubmnRqmt.InsCarrPlanId)
}

func (psr *PlanSubmnRqmtProcessor) handleClaimParms(planSubmnRqmt *PlanSubmnRqmt) error {
    count, err := psr.repository.GetClaimParmCount(psr.ctx, psr.tx, planSubmnRqmt.ClaimParmNbr)
    if err != nil {
        psr.logger.Error("Error getting claim parm count", constants.LoggingKeyCarrierCoId, planSubmnRqmt.CarrierCoId, constants.LoggingKeyInsCarrPlanId, planSubmnRqmt.InsCarrPlanId)
        return err
    }

    if count > 0 {
        return psr.repository.InsertPlanSubmnRqmt(psr.ctx, psr.tx, planSubmnRqmt)
    }
    return psr.repository.InsertD0PlanSubmnRqmt(psr.ctx, psr.tx, planSubmnRqmt)
}
