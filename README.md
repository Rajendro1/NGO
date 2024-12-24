func (icp *InsCarrierPlanProcessor) Process(packageDir string) (bool, error) {
	var carrExpirationDate string

	unlFile := fmt.Sprintf(constants.UnlFileInsCarrierPlan, packageDir, icp.fileMetadata.FileId)

	icp.logger.Info("Loading unl file", constants.UnlFileInsCarrierPlan, unlFile, constants.LoggingKeyFileId, icp.fileMetadata.FileId)
	fileData, err := icp.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		icp.logger.Error("Error loading the unl file", constants.LoggingKeyUnlFileName, unlFile)
		return false, err
	}

	if icp.groupPlanOperation == 1 {
		unlFile = fmt.Sprintf(constants.UnlFileStoreExpirationDate, packageDir, icp.fileMetadata.RequestId)
		icp.logger.Info("Loading unl file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, icp.fileMetadata.FileId)
		expirationFileData, err := icp.fileUtils.ReadUnlFiledata(unlFile)
		if err != nil {
			icp.logger.Warn("Error loading the unl file", constants.LoggingKeyUnlFileName, unlFile)
		}

		var storeNum int
		for _, row := range expirationFileData {
			for index, value := range row {
				switch index {
				case 0:
					storeNum, _ = strconv.Atoi(value)
				case 1:
					carrExpirationDate = value
				}
			}
			if fmt.Sprint(storeNum) == icp.storeNum && storeNum > 0 {
				break
			}
		}
	}

	for _, row := range fileData {
		insCarrPlan, err := NewInsCarrPlan(row, carrExpirationDate)
		if err != nil {
			icp.logger.Error("Error creating ins carr plan object", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
			return false, err
		}

		var count int
		if count, err = icp.repository.GetPlanCount(icp.ctx, icp.tx, insCarrPlan.CarrierCoId, insCarrPlan.InsCarrPlanId); err != nil {
			icp.logger.Error("Error getting plan count", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
			return false, err
		}

		if count == 0 {
			if err = icp.repository.InsertInsCarrPlan(icp.ctx, icp.tx, insCarrPlan); err != nil {
				icp.logger.Error("Error inserting ins carr plan", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
				return false, err
			}
		} else {
			if err = icp.repository.UpdateInsCarrPlan(icp.ctx, icp.tx, insCarrPlan); err != nil {
				icp.logger.Error("Error updating ins carr plan", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
				return false, err
			}
		}

		if err := icp.repository.DeleteCovgCopay(icp.ctx, icp.tx, insCarrPlan.CarrierCoId, insCarrPlan.InsCarrPlanId); err != nil {
			icp.logger.Error("Error deleting covg copay", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
			return false, err
		}
		if err := icp.repository.DeleteCovgOptRestrict(icp.ctx, icp.tx, insCarrPlan.CarrierCoId, insCarrPlan.InsCarrPlanId); err != nil {
			icp.logger.Error("Error deleting covrg opt restrict", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
			return false, err
		}
		if err := icp.repository.DeleteCarrPlanParm(icp.ctx, icp.tx, insCarrPlan.CarrierCoId, insCarrPlan.InsCarrPlanId); err != nil {
			icp.logger.Error("Error deleting carr plan parm", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
			return false, err
		}
		if err := icp.repository.DeleteCovgFee(icp.ctx, icp.tx, insCarrPlan.CarrierCoId, insCarrPlan.InsCarrPlanId); err != nil {
			icp.logger.Error("Error deleting covg fee", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
			return false, err
		}
		if err := icp.repository.DeleteCovgPrice(icp.ctx, icp.tx, insCarrPlan.CarrierCoId, insCarrPlan.InsCarrPlanId); err != nil {
			icp.logger.Error("Error deleting covg price", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
			return false, err
		}
		if err := icp.repository.DeleteCovgOption(icp.ctx, icp.tx, insCarrPlan.CarrierCoId, insCarrPlan.InsCarrPlanId); err != nil {
			icp.logger.Error("Error deleting covg option", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
			return false, err
		}
		if err := icp.repository.DeleteCarrGrp(icp.ctx, icp.tx, insCarrPlan.CarrierCoId, insCarrPlan.InsCarrPlanId); err != nil {
			icp.logger.Error("Error deleting carr grp", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
			return false, err
		}
		if err := icp.repository.DeleteParentCarrGrp(icp.ctx, icp.tx, insCarrPlan.CarrierCoId, insCarrPlan.InsCarrPlanId); err != nil {
			icp.logger.Error("Error deleting parent carr grp", constants.LoggingKeyCarrierCoId, insCarrPlan.CarrierCoId, constants.LoggingKeyInsCarrPlanId, insCarrPlan.InsCarrPlanId)
			return false, err
		}
	}

	return true, nil
}
