func (cir *CarrItemRestrictProcessor) Process(packageDir string) (bool, error) {
	var (
		previousCarrierCoId      int
		previousInsCarrPlanId    int
		previousCoverageOptionId int
		previousItemMdsFamId     int
		err                      error
	)

	//loading the file data
	unlFile := fmt.Sprintf(constants.UnlFileCarrierItemRestrict, packageDir, cir.fileMetadata.FileId)

	cir.logger.Info("Loading unl file", constants.LoggingKeyUnlFileName, unlFile, constants.LoggingKeyFileId, cir.fileMetadata.FileId)
	fileData, err := cir.fileUtils.ReadUnlFiledata(unlFile)
	if err != nil {
		cir.logger.Error("Error loading the unl file", constants.LoggingKeyUnlFileName, unlFile)
		return false, err
	}

	for _, row := range fileData {

		//validating primary keys and setting the model object
		carrItemRestrict, err := NewCarrItemRestrict(row)
		if err != nil {
			cir.logger.Error("Error creating carr item restrict object", constants.LoggingKeyCarrierCoId, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId)
			return false, err
		}

		var count int
		if carrItemRestrict.CarrierCoId != previousCarrierCoId || carrItemRestrict.InsCarrPlanId != previousInsCarrPlanId || carrItemRestrict.CoverageOptionId != previousCoverageOptionId || carrItemRestrict.ItemMdsFamId != previousItemMdsFamId {
			previousCarrierCoId = carrItemRestrict.CarrierCoId
			previousInsCarrPlanId = carrItemRestrict.InsCarrPlanId
			previousCoverageOptionId = carrItemRestrict.CoverageOptionId
			previousItemMdsFamId = carrItemRestrict.ItemMdsFamId

			if count, err = cir.repository.GetItemRestrictCount(cir.ctx, cir.tx, carrItemRestrict.CarrierCoId, carrItemRestrict.InsCarrPlanId, carrItemRestrict.CoverageOptionId, carrItemRestrict.ItemMdsFamId); err != nil {
				cir.logger.Error("Error getting item restrict count", constants.LoggingKeyCarrierCoId, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId, constants.LoggingKeyCoverageOptionId, carrItemRestrict.CoverageOptionId, constants.LoggingKeyItemMdsFamId, carrItemRestrict.ItemMdsFamId)
				return false, err
			}
			if count > 0 {
				if err = cir.repository.DeleteCarrItemRestrict(cir.ctx, cir.tx, carrItemRestrict.CarrierCoId, carrItemRestrict.InsCarrPlanId, carrItemRestrict.CoverageOptionId, carrItemRestrict.ItemMdsFamId); err != nil {
					cir.logger.Error("Error deleting carr item restrict", constants.LoggingKeyCarrierCoId, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId, constants.LoggingKeyCoverageOptionId, carrItemRestrict.CoverageOptionId, constants.LoggingKeyItemMdsFamId, carrItemRestrict.ItemMdsFamId)
					return false, err
				}
			}
		}

		if count, err = cir.repository.GetPlanCount(cir.ctx, cir.tx, carrItemRestrict.CarrierCoId, carrItemRestrict.InsCarrPlanId); err != nil {
			cir.logger.Error("Error getting plan count", constants.LoggingKeyCarrierCoId, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId)
			return false, err
		}
		if count <= 0 {
			cir.logger.Warn("Plan does not exist, hence continuing with next item restrict", constants.LoggingKeyCarrierCoId, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId)
			continue
		}
		if count, err = cir.repository.GetItemCount(cir.ctx, cir.tx, carrItemRestrict.ItemMdsFamId); err != nil {
			cir.logger.Error("Error getting item count", constants.LoggingKeyItemMdsFamId, carrItemRestrict.ItemMdsFamId)
			return false, err
		}
		if count <= 0 {
			cir.logger.Warn("Item does not exist, hence continuing with next item restrict", constants.LoggingKeyItemMdsFamId, carrItemRestrict.ItemMdsFamId)
			continue
		}
		if cir.subjectArea != constants.SubjectAreaItemTpRmpkg {
			if err = cir.repository.InsertCarrItemRestrict(cir.ctx, cir.tx, carrItemRestrict); err != nil {
				cir.logger.Error("Error inserting carr item restrict", constants.LoggingKeyCarrierCoId, carrItemRestrict.CarrierCoId, constants.LoggingKeyInsCarrPlanId, carrItemRestrict.InsCarrPlanId)
				return false, err
			}
		}
	}

	return true, nil
}
