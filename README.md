# Block Blob Latency Profiler

This application provides a means to profile client-observed latencies when issuing different block blob-related operations.  Output is reported in csv files for analysis.
The functions provided by test tool are:
1. **UploadTestBlobs** Concurrently uploads blobs for use by read-oriented tests.
2. **PutBlobTest** Issues PutBlob operations of a set size.  The latency of each operation is recorded for further analysis.
3. **PutBlockTest** Issues PutBlock operations of a set size.  The latency of each operation is recorded for further analaysis.
4. **RandownDownloadTest** Issues download operations against randomly-chosen blobs to simulate production workflows where reads are non-sequential.  The latency of each operation is recorded for further analysis.
5. **ListBlobsTest** Issues list operations against a set of existing blobs.  The latency of each operation is recorded for further analaysis.

## Prerequisites

**NOTE** For best performance, this application should be run atop .NET Core 2.1 or later.

* Install .NET core 2.1 for [Linux](https://www.microsoft.com/net/download/linux) or [Windows](https://www.microsoft.com/net/download/windows)

If you don't have an Azure subscription, create a [free account](https://azure.microsoft.com/free/?WT.mc_id=A261C142F) before you begin.

## Create a storage account using the Azure portal

First, create a new general-purpose storage account to use for this quickstart.

1. Go to the [Azure portal](https://portal.azure.com) and log in using your Azure account. 
2. On the Hub menu, select **New** > **Storage** > **Storage account - blob, file, table, queue**. 
3. Enter a unique name for your storage account. Keep these rules in mind for naming your storage account:
    - The name must be between 3 and 24 characters in length.
    - The name may contain numbers and lowercase letters only.
4. Make sure that the following default values are set: 
    - **Deployment model** is set to **Resource manager**.
    - **Account kind** is set to **General purpose**.
    - **Performance** is set to **Standard**.
    - **Replication** is set to **Locally Redundant storage (LRS)**.
5. Select your subscription. 
6. For **Resource group**, create a new one and give it a unique name. 
7. Select the **Location** to use for your storage account.
8. Check **Pin to dashboard** and click **Create** to create your storage account. 

After your storage account is created, it is pinned to the dashboard. Click on it to open it. Under **Settings**, click **Access keys**. Select the primary key and copy the associated **Connection string** to the clipboard, then paste it into a text editor for later use.  Note that we are directly using storage account keys in this example for the sake of simplicity.  It is highly recommended to use another means to authenticate with the storage service in production environments.

## Put the connection string in an environment variable

This project requires that the storage account and a SAS token with read/write priviliges on the account are stored in environment variables securely on the machine running the test.

| Name         | Description                                                |
| ------------ | ---------------------------------------------------------- |
| [envName]    | The name of the storage account to use.                    |
| [envName]Sas | The SAS token of the storage account defined in [envName]. |

Follow one of the examples below depending on your Operating System to create the environment variable. If using windows close out of your open IDE or shell and restart it to be able to read the environment variable.

### Linux

```bash
export storageAcc = "<your storage account name>"
export storageAccSas = "<your storage account's SAS token>"
```
### Windows

```cmd
setx storageAcc "<your storage account name>"
setx storstorageAccSasageAcc "<your storage account's SAS token>"
```

At this point, you can run this application.

## Running the application

To run the command, navigate to the base directory of the repository and run `dotnet run` with the appropriate arguments.  This project can be run in several modes and as such has arguments for the base command, followed by arguments specific to the mode being executed.

### Base Command-Level Arguments

- `arg0: Container Name`
Specifies the name of the container to be used.
- `arg1: Storage Account Name Environment Variable`
The name of the environment variable containing the name of the storage account to be used.  Note: The SAS token must be specified in a second variable with the same name followed by a 'Sas' suffix.
- `arg2: Output File Path`
The path to the output file.  The output will be in a CSV format.
- `arg3: Environment` (prod|preprod|test)
The Azure environment the storage account exists within.
- `arg4: Use HTTPS` (true|false)
Indicates whether to use HTTPS (true) or HTTP (false).
- `arg5: Test Type` (UploadTestBlobs|PutBlobTest|PutBlockTest|RandomDownloadTest|ListBlobsTest)
The name of the specific test (mode) to run.

### Mode-Specific Arguments

#### UploadTestBlobs

In this mode, blobs are concurrently uploaded with a common prefix followed by an index (0 - n-1).  This naming convention is used by get-related test operations.

- `arg0: Blob Prefix`
Specifies the common prefix used for naming blobs created by this mode.
- `arg1: Number of Blobs to Upload`
Specifies the number of blobs to be uploaded.
- `arg2: Blob Size (bytes)`
The size (in bytes) of each blob to upload.  This must be between 0 and the maximum size of a Blob (~4.8 TiB).

##### Example
```
dotnet run testcontainer storageacc ./results.csv prod true **UploadTestBlobs testblob- 1000000 4194304**
```
The application will upload 1,000,000 4MiB blobs named **testblob-0** through **testblob-999999** to the storage account identified in the environment variable **storageacc** using the SAS token in environment variable **storageaccSas**.  The storage account is housed in a production tenant and we're configured to use HTTPS.  Output is written to ./results.csv (though no performance is tracked).

#### PutBlobTest

In this mode, blobs are sequentially uploaded with a common prefix followed by an index (0 - n-1).  This naming convention is used by get-related test operations.

- `arg0: Blob Prefix`
Specifies the common prefix used for naming blobs created by this mode.
- `arg1: Number of Blobs to Upload`
Specifies the number of blobs to be uploaded.
- `arg2: Blob Size (bytes)`
The size (in bytes) of each blob to upload.  This must be between 0 and the maximum size of a Blob (~4.8 TiB).
- `arg3: [Optional] Report Frequency`
The number of blobs between each perf status update to the console.
- `arg4: [Optional] Percentile`
The percentile to track for perf status updates to the console.  99 = 99%, 99.9 = 99.9%

##### Example
```
dotnet run testcontainer storageacc ./results.csv prod true **PutBlobTest testblob- 1000000 4194304 100 99**
```
The application will upload 1,000,000 4MiB blobs named **testblob-0** through **testblob-999999** to the storage account identified in the environment variable **storageacc** using the SAS token in environment variable **storageaccSas**.  The storage account is housed in a production tenant and we're configured to use HTTPS.  Output is written to ./results.csv with the latency of each blob upload.  The total average and 99th percentile latencies are reported after every 100 blobs.

#### PutBlockTest

In this mode, blocks are sequentially uploaded to blobs named with a given prefix followed by an index.

- `arg0: Blob Prefix`
Specifies the common prefix used for naming blobs created by this mode.
- `arg1: Number of Blocks to Upload`
Specifies the number of blobs to be uploaded.
- `arg2: Block Size (bytes)`
The size (in bytes) of each block to upload.  This must be between 0 and 100 MiB (the maximum size of a Block).
- `arg3: [Optional] Report Frequency`
The number of blocks between each perf status update to the console.
- `arg4: [Optional] Percentile`
The percentile to track for perf status updates to the console.  99 = 99%, 99.9 = 99.9%

##### Example
```
dotnet run testcontainer storageacc ./results.csv prod true **PutBlockTest testblob- 1000000 4194304 100 99**
```
The application will upload 100,000 4MiB blocks to **testblob-0** and 10,000 4MiB blocks to **testblob-1** to the storage account identified in the environment variable **storageacc** using the SAS token in environment variable **storageaccSas**.  The storage account is housed in a production tenant and we're configured to use HTTPS.  Output is written to ./results.csv with the latency of each block upload.  The total average and 99th percentile latencies are reported after every 100 blocks.

#### RandomDownloadTest

In this mode, blobs are randomly downloaded in series.

- `arg0: Blob Prefix`
Specifies the common prefix used for naming blobs to be downloaded by this mode.
- `arg1: Number of Blobs to Download`
Specifies the number of blobs to download.
- `arg2: Number of Bytes to Download per Blob`
The size (in bytes) to download from each blob.
- `arg3: Number of Blobs that are Available in the given Container`
The number of blobs to randomly sample.  If 1000, blobs named [prefix]0 through [prefix]999 are randomly selected.
- `arg4: [Optional] Report Frequency`
The number of blobs between each perf status update to the console.
- `arg5: [Optional] Percentile`
The percentile to track for perf status updates to the console.  99 = 99%, 99.9 = 99.9%

##### Example
```
dotnet run testcontainer storageacc ./results.csv prod true **RandomDownloadTest testblob- 100000 65536 1000000 100 99**
```
The application will download the first 64KiB of 100,000 blobs randomly chosen out of an available set of 1,000,000 (named **testblob-0** through **testblob-999999**) from the storage account identified in the environment variable **storageacc** using the SAS token in environment variable **storageaccSas**.  The storage account is housed in a production tenant and we're configured to use HTTPS.  Output is written to ./results.csv with the latency of each block upload.  The total average and 99th percentile latencies are reported after every 100 blobs.

#### ListBlobsTest

In this mode, blobs matching a given prefix are listed a given number of times.

- `arg0: Blob Prefix`
Specifies the common prefix used for naming blobs to be listed by this mode.
- `arg1: Number of List Operations`
Specifies the number of list operations to perform.
- `arg2: [Optional] Report Frequency`
The number of list oeprations between each perf status update to the console.
- `arg3: [Optional] Percentile`
The percentile to track for perf status updates to the console.  99 = 99%, 99.9 = 99.9%

##### Example
```
dotnet run testcontainer storageacc ./results.csv prod true **ListBlobsTest testblob- 1000 100 99**
```
The application will perform 1,000 list oeprations against **testcontainer** for blobs prefixed with **testblob-** in the storage account identified in the environment variable **storageacc** using the SAS token in environment variable **storageaccSas**.  The storage account is housed in a production tenant and we're configured to use HTTPS.  Output is written to ./results.csv with the latency of each block upload.  The total average and 99th percentile latencies are reported after every 100 list operations.



## More information

The [Azure storage documentation](https://docs.microsoft.com/azure/storage/) includes a rich set of tutorials and conceptual articles, which serve as a good complement to the samples.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
