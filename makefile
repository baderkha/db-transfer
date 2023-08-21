
BUILD_PATH=./build
init:
	mkdir -p ${BUILD_PATH}
	mkdir -p ${BUILD_PATH}/resources
	cp test_run.json  ${BUILD_PATH}/job.json
	cp -r resources/. ${BUILD_PATH}/resources/.
start: init build-bin-mac
	cd ${BUILD_PATH} && AWS_SDK_LOAD_CONFIG=1 AWS_DEFAULT_PROFILE=awseabyvdev ./db_transfer 
build-bin-mac: init
	GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -o $(BUILD_PATH)/db_transfer              ./cmd/local/main1.go
build-bin-linux:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o $(BUILD_PATH)/db_transfer              ./cmd/local/main1.go
