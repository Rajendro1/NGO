func InitMockConfig(t *testing.T, schemaPath string) (sqlmock.Sqlmock, func()) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	gormDB, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})
	assert.NoError(t, err)

	// Set the AppConfig for testing
	config.AppConfig = &config.Config{
		DB:         gormDB,
		SchemaPath: schemaPath,
	}

	cleanup := func() {
		db.Close()
	}

	return mock, cleanup
}

func TestCreateUserWithSchemaValidation(t *testing.T) {
	schemaPath := "./schema/user_schema.json"
	mock, cleanup := InitMockConfig(t, schemaPath)
	defer cleanup()

	type testCase struct {
		name           string
		requestBody    interface{}
		mockDBBehavior func()
		expectedStatus int
		expectedBody   string
	}

	tests := []testCase{
		{
			name: "Success Case",
			requestBody: handler.User{
				Name:  "John Doe",
				Email: "john.doe@example.com",
			},
			mockDBBehavior: func() {
				mock.ExpectBegin()
				mock.ExpectQuery(`INSERT INTO "users"`).WithArgs("John Doe", "john.doe@example.com").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
				mock.ExpectExec(`INSERT INTO "user_profiles"`).WithArgs(1, "This is the user bio").WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			},
			expectedStatus: http.StatusCreated,
			expectedBody:   `"id":1`,
		},
		{
			name: "Missing Required Field",
			requestBody: map[string]string{
				"email": "john.doe@example.com",
			},
			mockDBBehavior: func() {},
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "JSON validation failed",
		},
		{
			name: "Invalid Email Format",
			requestBody: handler.User{
				Name:  "John Doe",
				Email: "invalid-email",
			},
			mockDBBehavior: func() {},
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "does not match format",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jsonBody, _ := json.Marshal(tc.requestBody)

			req, err := http.NewRequest("POST", "/users", bytes.NewBuffer(jsonBody))
			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()

			// Apply DB behavior
			tc.mockDBBehavior()

			// Call the handler
			handler.CreateUserWithSchemaValidation(rr, req)

			// Assertions
			assert.Equal(t, tc.expectedStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tc.expectedBody)

			// Verify all DB expectations
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
