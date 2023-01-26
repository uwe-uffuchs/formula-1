# Databricks notebook source
storageAccountName = "sadlformula1"
keyVaultScopeName = "formula 1"
clientId = dbutils.secrets.get(scope = keyVaultScopeName, key = "databricks-app-clientId")
tenantId = dbutils.secrets.get(scope = keyVaultScopeName, key = "databricks-app-tenantId")
clientSecret = dbutils.secrets.get(scope = keyVaultScopeName, key = "databricks-app-clientSecret")
containersToMount = "bronze,silver,gold"
containersToUnmount = "raw,processed"

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": f"{clientId}",
    "fs.azure.account.oauth2.client.secret": f"{clientSecret}",
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantId}/oauth2/token"
}

# COMMAND ----------

# string containersList = comma seperated list of containers to either mount or unmount
# bit unmountOrMount = 1 to mount, 0 to unmount
def mount_logic(containersList,unmountOrMount):
    if (unmountOrMount <= 1 and unmountOrMount >= 0):
        if unmountOrMount == 1:
            for container in containersList.split(","):
                if not any(mount.mountPoint == f"/mnt/{storageAccountName}/{container}" for mount in dbutils.fs.mounts()):
                    dbutils.fs.mount(
                        source = f"abfss://{container}@{storageAccountName}.dfs.core.windows.net/",
                        mount_point = f"/mnt/{storageAccountName}/{container}",
                        extra_configs = configs
                    )
        else:
            for container in containersList.split(","):
                if any(mount.mountPoint == f"/mnt/{storageAccountName}/{container}" for mount in dbutils.fs.mounts()):
                    dbutils.fs.unmount(f"/mnt/{storageAccountName}/{container}")
    else:
        print("Invalid argument")

# COMMAND ----------

mount_logic(containersToMount, 1)
mount_logic(containersToUnmount, 0)

# COMMAND ----------

display(dbutils.fs.mounts())
