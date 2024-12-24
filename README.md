func (icp *InsCarrierPlanProcessor) Process(packageDir string) (bool, error) {
	var carrExpirationDate string

	// Load primary unl file
	fileData, err := icp.loadFileData(constants.UnlFileInsCarrierPlan, packageDir, icp.fileMetadata.FileId)
	if err != nil {
		return false, err
	}

	// Load expiration data if applicable
	if icp.groupPlanOperation == 1 {
		carrExpirationDate, err = icp.getExpirationDate(packageDir)
		if err != nil {
			icp.logger.Warn("Error fetching expiration date", constants.LoggingKeyFileId, icp.fileMetadata.FileId)
		}
	}

	// Process each row in the file
	for _, row := range fileData {
		if err := icp.processRow(row, carrExpirationDate); err != nil {
			return false, err
		}
	}

	return true, nil
}

// Helper function to load file data
func (icp *InsCarrierPlanProcessor) loadFileData(fileConstant, packageDir, identifier string) ([][]string, error) {
	unlFile := fmt.Sprintf(fileConstant, packageDir, identifier)
	icp.logger.Info("Loading unl file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, identifier)

	fileData, err := icp.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		icp.logger.Error("Error loading the unl file", constants.LoggingKeyUnlFileName, unlFile)
		return nil, err
	}

	return fileData, nil
}

// Helper function to fetch expiration date
func (icp *InsCarrierPlanProcessor) getExpirationDate(packageDir string) (string, error) {
	unlFile := fmt.Sprintf(constants.UnlFileStoreExpirationDate, packageDir, icp.fileMetadata.RequestId)
	icp.logger.Info("Loading expiration file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, icp.fileMetadata.FileId)

	expirationFileData, err := icp.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		return "", err
	}

	for _, row := range expirationFileData {
		if len(row) < 2 {
			continue
		}
		storeNum, _ := strconv.Atoi(row[0])
		if fmt.Sprint(storeNum) == icp.storeNum && storeNum > 0 {
			return row[1], nil
		}
	}

	return "", nil
}

// Helper function to process a single row
func (icp *InsCarrierPlanProcessor) processRow(row []string, expirationDate string) error {
	insCarrPlan, err := NewInsCarrPlan(row, expirationDate)
	if err != nil {
		icp.logger.Error("Error creating ins carr plan object", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
		return err
	}

	count, err := icp.repository.GetPlanCount(icp.ctx, icp.tx, insCarrPlan.CarrierCoId, insCarrPlan.InsCarrPlanId)
	if err != nil {
		icp.logger.Error("Error getting plan count", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
		return err
	}

	if err := icp.insertOrUpdatePlan(count, insCarrPlan); err != nil {
		return err
	}

	return icp.cleanupRelatedData(insCarrPlan)
}

// Helper function to insert or update plan
func (icp *InsCarrierPlanProcessor) insertOrUpdatePlan(count int, plan *InsCarrPlan) error {
	if count == 0 {
		if err := icp.repository.InsertInsCarrPlan(icp.ctx, icp.tx, plan); err != nil {
			icp.logger.Error("Error inserting ins carr plan", constants.LoggingKeyCarrierCoId, plan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, plan.InsCarrPlanId)
			return err
		}
	} else {
		if err := icp.repository.UpdateInsCarrPlan(icp.ctx, icp.tx, plan); err != nil {
			icp.logger.Error("Error updating ins carr plan", constants.LoggingKeyCarrierCoId, plan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, plan.InsCarrPlanId)
			return err
		}
	}
	return nil
}

// Helper function to clean up related data
func (icp *InsCarrierPlanProcessor) cleanupRelatedData(plan *InsCarrPlan) error {
	deleteFunctions := []func() error{
		func() error { return icp.repository.DeleteCovgCopay(icp.ctx, icp.tx, plan.CarrierCoId, plan.InsCarrPlanId) },
		func() error { return icp.repository.DeleteCovgOptRestrict(icp.ctx, icp.tx, plan.CarrierCoId, plan.InsCarrPlanId) },
		func() error { return icp.repository.DeleteCarrPlanParm(icp.ctx, icp.tx, plan.CarrierCoId, plan.InsCarrPlanId) },
		func() error { return icp.repository.DeleteCovgFee(icp.ctx, icp.tx, plan.CarrierCoId, plan.InsCarrPlanId) },
		func() error { return icp.repository.DeleteCovgPrice(icp.ctx, icp.tx, plan.CarrierCoId, plan.InsCarrPlanId) },
		func() error { return icp.repository.DeleteCovgOption(icp.ctx, icp.tx, plan.CarrierCoId, plan.InsCarrPlanId) },
		func() error { return icp.repository.DeleteCarrGrp(icp.ctx, icp.tx, plan.CarrierCoId, plan.InsCarrPlanId) },
		func() error { return icp.repository.DeleteParentCarrGrp(icp.ctx, icp.tx, plan.CarrierCoId, plan.InsCarrPlanId) },
	}

	for _, deleteFunc := range deleteFunctions {
		if err := deleteFunc(); err != nil {
			icp.logger.Error("Error during cleanup", constants.LoggingKeyCarrierCoId, plan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, plan.InsCarrPlanId)
			return err
		}
	}

	return nil
}
