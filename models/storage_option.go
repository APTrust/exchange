package models

import (
	"fmt"
)

type StorageOption struct {
	Name             string
	Provider         string
	Endpoint         string
	EnvKeyIdName     string
	EnvSecretKeyName string
	StorageClass     string
	AppendBucketName bool
	UseSSL           bool
}

var StorageOptions = map[string]StorageOption{
	"Standard-S3": {
		Name:             "Standard-S3",
		Provider:         "AWS",
		Endpoint:         "https://aptrust.preservation.storage.s3.amazonaws.com",
		EnvKeyIdName:     "AWS_ACCESS_KEY_ID",
		EnvSecretKeyName: "AWS_SECRET_ACCESS_KEY",
		StorageClass:     "STANDARD",
		AppendBucketName: false,
		UseSSL:           true,
	},
	"Standard-Glacier": {
		Name:             "Standard-Glacier",
		Provider:         "AWS",
		Endpoint:         "https://aptrust.preservation.oregon.s3.amazonaws.com",
		EnvKeyIdName:     "AWS_ACCESS_KEY_ID",
		EnvSecretKeyName: "AWS_SECRET_ACCESS_KEY",
		StorageClass:     "GLACIER",
		AppendBucketName: false,
		UseSSL:           true,
	},
	"Glacier-VA": {
		Name:             "Glacier-VA",
		Provider:         "AWS",
		Endpoint:         "https://aptrust.preservation.glacier.va.s3.amazonaws.com",
		EnvKeyIdName:     "AWS_ACCESS_KEY_ID",
		EnvSecretKeyName: "AWS_SECRET_ACCESS_KEY",
		StorageClass:     "GLACIER",
		AppendBucketName: false,
		UseSSL:           true,
	},
	"Glacier-OH": {
		Name:             "Glacier-OH",
		Provider:         "AWS",
		Endpoint:         "https://aptrust.preservation.glacier.oh.s3.amazonaws.com",
		EnvKeyIdName:     "AWS_ACCESS_KEY_ID",
		EnvSecretKeyName: "AWS_SECRET_ACCESS_KEY",
		StorageClass:     "GLACIER",
		AppendBucketName: false,
		UseSSL:           true,
	},
	"Glacier-OR": {
		Name:             "Glacier-OR",
		Provider:         "AWS",
		Endpoint:         "https://aptrust.preservation.glacier.or.s3.amazonaws.com",
		EnvKeyIdName:     "AWS_ACCESS_KEY_ID",
		EnvSecretKeyName: "AWS_SECRET_ACCESS_KEY",
		StorageClass:     "GLACIER",
		AppendBucketName: false,
		UseSSL:           true,
	},
	"Glacier-Deep-VA": {
		Name:             "Glacier-Deep-VA",
		Provider:         "AWS",
		Endpoint:         "https://aptrust.preservation.glacier-deep.va.s3.amazonaws.com",
		EnvKeyIdName:     "AWS_ACCESS_KEY_ID",
		EnvSecretKeyName: "AWS_SECRET_ACCESS_KEY",
		StorageClass:     "DEEP_ARCHIVE",
		AppendBucketName: false,
		UseSSL:           true,
	},
	"Glacier-Deep-OH": {
		Name:             "Glacier-Deep-OH",
		Provider:         "AWS",
		Endpoint:         "https://aptrust.preservation.glacier-deep.oh.s3.amazonaws.com",
		EnvKeyIdName:     "AWS_ACCESS_KEY_ID",
		EnvSecretKeyName: "AWS_SECRET_ACCESS_KEY",
		StorageClass:     "DEEP_ARCHIVE",
		AppendBucketName: false,
		UseSSL:           true,
	},
	"Glacier-Deep-OR": {
		Name:             "Glacier-Deep-OR",
		Provider:         "AWS",
		Endpoint:         "https://aptrust.preservation.glacier-deep.or.s3.amazonaws.com",
		EnvKeyIdName:     "AWS_ACCESS_KEY_ID",
		EnvSecretKeyName: "AWS_SECRET_ACCESS_KEY",
		StorageClass:     "DEEP_ARCHIVE",
		AppendBucketName: false,
		UseSSL:           true,
	},
	"Wasabi-VA": {
		Name:             "Wasabi-VA",
		Provider:         "Wasabi",
		Endpoint:         "https://s3.us-east-1.wasabisys.com",
		EnvKeyIdName:     "WASABI_ACCESS_KEY_ID",
		EnvSecretKeyName: "WASABI_SECRET_ACCESS_KEY",
		StorageClass:     "STANDARD",
		AppendBucketName: true,
		UseSSL:           true,
	},
	"Wasabi-OR": {
		Name:             "Wasabi-OR",
		Provider:         "Wasabi",
		Endpoint:         "https://s3.us-west-1.wasabisys.com",
		EnvKeyIdName:     "WASABI_ACCESS_KEY_ID",
		EnvSecretKeyName: "WASABI_SECRET_ACCESS_KEY",
		StorageClass:     "STANDARD",
		AppendBucketName: true,
		UseSSL:           true,
	},
}

// NEEDS TEST
func GetStorageOption(storageOptionName string) (storageOption StorageOption, err error) {
	storageOption, optionExists := StorageOptions[storageOptionName]
	if !optionExists {
		err = fmt.Errorf("No storage option record for '%s'", storageOptionName)
	}
	return storageOption, err
}
