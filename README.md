
func (pcp *PhmCarrProcessor) Process(packageDir string) (bool, error) {
	unlFile := fmt.Sprintf(constants.UnlFilePhmCarrProcessor, packageDir, pcp.fileMetadata.FileId)

	pcp.logger.Info("Loading unl file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, pcp.fileMetadata.FileId)
	fileData, err := pcp.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		pcp.logger.Error("Error loading the unl file", constants.LoggingKeyUnlFileName, unlFile)
		return false, err
	}

	for _, row := range fileData {
		phmCarr, err := NewPhmCarr(row)
		if err != nil {
			pcp.logger.Error("Error creating phm carr object", constants.LoggingKeyCarrierCoId, phmCarr.CarrierCoId, constants.LoggingKeyInsCarrPlanId, phmCarr.InsCarrPlanId)
			return false, err
		}

		var count int
		if count, err = pcp.repository.GetPlanCount(pcp.ctx, pcp.tx, phmCarr.CarrierCoId, phmCarr.InsCarrPlanId); err != nil {
			pcp.logger.Error("Error getting plan count", constants.LoggingKeyCarrierCoId, phmCarr.CarrierCoId, constants.LoggingKeyInsCarrPlanId, phmCarr.InsCarrPlanId)
			return false, err
		}

		if count == 0 {
			pcp.logger.Error("No existing plans, hence skipping phm carr file processing", constants.LoggingKeyCarrierCoId, phmCarr.CarrierCoId, constants.LoggingKeyInsCarrPlanId, phmCarr.InsCarrPlanId)
			return true, fmt.Errorf("skipping phmCarr record processing as plan does not exist with CarrierCoId: %d and InsCarrPlan %d", phmCarr.CarrierCoId, phmCarr.InsCarrPlanId)
		} else {
			if count, err = pcp.repository.GetFamCount(pcp.ctx, pcp.tx, phmCarr.ProcessFamNbr); err != nil {
				return false, err
			}
			if count == 0 {
				pcp.logger.Error("No existing process fam nbr, hence skipping phm carr file processing", "processFamNbr", phmCarr.ProcessFamNbr)
				return true, fmt.Errorf("skipping phmCarr record processing as Fam does not exist with ProcessFamNbr %d", phmCarr.ProcessFamNbr)
			}
		}

		if count > 0 {
			phmCarr.ProviderNbr, err = pcp.repository.GetProvider(pcp.ctx, pcp.tx, phmCarr.CarrierCoId, phmCarr.InsCarrPlanId)
			if err != nil {
				pcp.logger.Error("Error getting provider", constants.LoggingKeyCarrierCoId, phmCarr.CarrierCoId, constants.LoggingKeyInsCarrPlanId, phmCarr.InsCarrPlanId)
				return false, err
			}

			if err = pcp.repository.DeletePhmCarr(pcp.ctx, pcp.tx, phmCarr.CarrierCoId, phmCarr.InsCarrPlanId); err != nil {
				pcp.logger.Error("Error deleting phm carr", constants.LoggingKeyCarrierCoId, phmCarr.CarrierCoId, constants.LoggingKeyInsCarrPlanId, phmCarr.InsCarrPlanId)
				return false, err
			}

			if err = pcp.repository.InsertPhmCarr(pcp.ctx, pcp.tx, phmCarr); err != nil {
				pcp.logger.Error("Error inserting phm carr", constants.LoggingKeyCarrierCoId, phmCarr.CarrierCoId, constants.LoggingKeyInsCarrPlanId, phmCarr.InsCarrPlanId)
				return false, err
			}
		}
	}
	return true, nil
}
