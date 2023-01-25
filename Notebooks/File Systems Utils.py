# Databricks notebook source
storageAccountName = "sadlformula1"
keyVaultScopeName = "formula 1"
clientId = dbutils.secrets.get(scope = keyVaultScopeName, key = "databricks-app-clientId")
tenantId = dbutils.secrets.get(scope = keyVaultScopeName, key = "databricks-app-tenantId")
clientSecret = dbutils.secrets.get(scope = keyVaultScopeName, key = "databricks-app-clientSecret")
rawContainerName = "raw"
processedContainerName = "processed"

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": f"{clientId}",
    "fs.azure.account.oauth2.client.secret": f"{clientSecret}",
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantId}/oauth2/token"
}

# COMMAND ----------

def mount_adls(container):
    if not any(mount.mountPoint == f"/mnt/{storageAccountName}/{container}" for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source = f"abfss://{container}@{storageAccountName}.dfs.core.windows.net/",
            mount_point = f"/mnt/{storageAccountName}/{container}",
            extra_configs = configs
        )

# COMMAND ----------

mount_adls(rawContainerName)
mount_adls(processedContainerName)
