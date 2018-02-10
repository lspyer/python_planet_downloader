A python script for downloading Planet Labs Inc. imagery

## install
```pip install planet```

```pip install joblib```
#### for Azure blob storage define the following:
```pip install azure-storage-blob```

## configure environment variables
```export PL_API_KEY=<put_your_api_key_here>```
#### for Azure blob storage define the following:
```export AZURE_BS_ACC_NAME=<put_your_account_name_here>```

```export AZURE_BS_API_KEY=<put_your_api_key_here>```

## run

### run to local storage
```python planet_downloader.py config_local_storage.json```

### run to blob storage
```python planet_downloader.py config_blob_storage.json```
