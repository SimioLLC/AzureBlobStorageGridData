using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using SimioAPI;
using SimioAPI.Extensions;

using System.Security.Policy;
using System.Net;
using Azure.Storage.Blobs;
using System.Runtime.InteropServices.ComTypes;
using System.Data;
using System.Runtime.Remoting.Contexts;
using System.Data.SqlTypes;
using System.Xml;
using Newtonsoft.Json;
using System.Diagnostics.Eventing.Reader;
using System.Runtime;

namespace AzureBlobStorageGridData
{
    public class ImporterDefinition : IGridDataImporterDefinition
    {
        public string Name => "Azure Blob Storage Data Importer";
        public string Description => "An importer for Azure Blob Storage formatted data";
        public Image Icon => null;

        static readonly Guid MY_ID = new Guid("9f378ea0-0162-4ebb-9ad1-17dca532ae3e");
        public Guid UniqueID => MY_ID;

        public IGridDataImporter CreateInstance(IGridDataImporterContext context)
        {
            return new Importer(context);
        }

        public void DefineSchema(IGridDataSchema schema)
        {
            var connectionString = schema.OverallProperties.AddStringProperty("ConnectionString");
            connectionString.DisplayName = "ConnectionString";
            connectionString.Description = "ConnectionString.";
            connectionString.DefaultValue = String.Empty;

            var passwordProp = schema.OverallProperties.AddCredentialProperty("Password");
            passwordProp.DisplayName = "Password";
            passwordProp.Description = "Optional password that can be referenced in connection string using ${password}.";
            passwordProp.DefaultValue = String.Empty;

            var containerName = schema.PerTableProperties.AddStringProperty("ContainerName");
            containerName.DisplayName = "ContainerName";
            containerName.Description = "ContainerName.";
            containerName.DefaultValue = String.Empty;

            var blobName = schema.PerTableProperties.AddStringProperty("BlobName");
            blobName.DisplayName = "BlobName";
            blobName.Description = "BlobName.";
            blobName.DefaultValue = String.Empty;

            var messageType = schema.PerTableProperties.AddListProperty("MessageType", new[] { "JSON", "XML", "OTHER" });
            messageType.DisplayName = "MessageType";
            messageType.Description = "MessageType.";
            messageType.DefaultValue = "JSON";

            var stylesheetProp = schema.PerTableProperties.AddXSLTProperty("Stylesheet");
            stylesheetProp.Description = "The transform to apply to the data returned from the download.";
            stylesheetProp.DefaultValue =
@"<xsl:stylesheet version=""1.0"" xmlns:xsl=""http://www.w3.org/1999/XSL/Transform"">
    <xsl:template match=""node()|@*"">
      <xsl:copy>
        <xsl:apply-templates select=""node()|@*""/>
      </xsl:copy>
    </xsl:template>
</xsl:stylesheet>";
            stylesheetProp.GetXML += StylesheetProp_GetXML;

            var debugFileFolder = schema.PerTableProperties.AddFilesLocationProperty("DebugFileFolder");
            debugFileFolder.DisplayName = "Debug File Folder";
            debugFileFolder.Description = "Debug File Folder.";
            debugFileFolder.DefaultValue = String.Empty;

            var editXMLStylesheetInputFile = schema.PerTableProperties.AddFileProperty("EditXMLStylesheetInputFile");
            editXMLStylesheetInputFile.DisplayName = "Edit XML Stylesheet Input File";
            editXMLStylesheetInputFile.Description = "Edit XML Stylesheet Input File.";
            editXMLStylesheetInputFile.DefaultValue = String.Empty;
        }

        private void StylesheetProp_GetXML(object sender, XSLTAddInPropertyGetXMLEventArgs e)
        {
            string debugFileFolder = string.Empty;
            string stylesheet = string.Empty;

            Importer.GetValues(e.HierarchicalProperties[0], e.OtherProperties, out var connectionString, out var containerName, out var blobName, out var messageType, ref debugFileFolder, ref stylesheet, out var editXMLStylesheetInputFile);

            if (editXMLStylesheetInputFile != null && editXMLStylesheetInputFile.Length > 0)
            {
                e.XML = File.ReadAllText(editXMLStylesheetInputFile);
            }
            else
            {   

                Importer.GetData(connectionString, containerName, blobName, out var resultString);

                if (messageType == "OTHER")
                {
                    resultString = "<data><![CDATA[" + resultString.TrimEnd().Replace("\\", String.Empty) + "]]></data>";
                }
                else if (messageType == "JSON")
                {
                    resultString = JsonConvert.DeserializeXmlNode(resultString).InnerXml;
                }
                e.XML = resultString;
            }
        }
    }

    class Importer : IGridDataImporter
    {
        public Importer(IGridDataImporterContext context)
        {
        }

        public OpenImportDataResult OpenData(IGridDataOpenImportDataContext openContext)
        {
            string debugFileFolder = string.Empty;
            string stylesheet = string.Empty;

            GetValues(openContext.Settings.Properties, openContext.Settings.GridDataSettings[openContext.TableName].Properties, out var connectionString, out var containerName, out var blobName, out var messageType, ref debugFileFolder, ref stylesheet, out var editXMLStylesheetInputFile);

            if (String.IsNullOrWhiteSpace(connectionString))
                return OpenImportDataResult.Failed("The Connection String parameter is not specified");

            if (String.IsNullOrWhiteSpace(containerName))
                return OpenImportDataResult.Failed("The Container Name parameter is not specified");

            if (String.IsNullOrWhiteSpace(blobName))
                return OpenImportDataResult.Failed("The Blob Name parameter is not specified");

            if (messageType != "JSON" && messageType != "XML" && messageType != "OTHER")
                return OpenImportDataResult.Failed("Invalid Message Type");

            if (GetData(connectionString, containerName, blobName, out var resultString) == false)
            {
                throw new Exception(resultString);
            }

            if (debugFileFolder.Length > 0)
            {
                Console.WriteLine("\nSaving blob to\n\t{0}\n", debugFileFolder + "\\" + blobName);

                File.WriteAllText(debugFileFolder + "\\" + blobName, resultString);

            }

            var mergedDataSet = new DataSet();
            int numberOfRows = 0;

            if (messageType == "OTHER")
            {
                resultString = "<data><![CDATA[" + resultString.Replace("\"", string.Empty) + "]]></data>";
            }
            else
            {
                XmlDocument xmlDoc = new XmlDocument();
                if (messageType == "JSON")
                {
                    xmlDoc = JsonConvert.DeserializeXmlNode(resultString);
                }
                else
                {
                    xmlDoc.LoadXml(resultString);
                }
                resultString = xmlDoc.InnerXml;
            }

            var result = Simio.Xml.XsltTransform.TransformXmlToDataSet(resultString, stylesheet, null, out var finalXMLString);
            if (result.XmlTransformError != null)
                return new OpenImportDataResult() { Result = GridDataOperationResult.Failed, Message = result.XmlTransformError };
            if (result.DataSetLoadError != null)
                return new OpenImportDataResult() { Result = GridDataOperationResult.Failed, Message = result.DataSetLoadError };
            if (result.DataSet.Tables.Count > 0) numberOfRows = result.DataSet.Tables[0].Rows.Count;
            else numberOfRows = 0;
            if (numberOfRows > 0)
            {
                result.DataSet.AcceptChanges();
                if (mergedDataSet.Tables.Count == 0) mergedDataSet.Merge(result.DataSet);
                else mergedDataSet.Tables[0].Merge(result.DataSet.Tables[0]);
                mergedDataSet.AcceptChanges();
            }

            // If no rows found by importer, create result data table with zero rows, but the same set of columns from the table so importer does not error out saying "no column names in data source match existing column names in table"
            if (mergedDataSet.Tables.Count == 0)
            {
                var zeroRowTable = new DataTable();
                var columnSettings = openContext.Settings.GridDataSettings[openContext.TableName]?.ColumnSettings;
                if (columnSettings != null)
                {
                    foreach (var cs in columnSettings)
                    {
                        zeroRowTable.Columns.Add(cs.ColumnName);
                    }
                }
                mergedDataSet.Tables.Add(zeroRowTable);
            }

            return new OpenImportDataResult()
            {
                Result = GridDataOperationResult.Succeeded,
                Records = new AzureBlobStorageGridDataRecords(mergedDataSet)
            };
            
        }

        internal static bool checkIsProbablyJSONObject(string resultString)
        {
            // We are looking for the first non-whitespace character (and are specifically not Trim()ing here
            //  to eliminate memory allocations on potentially large (we think?) strings
            foreach (var theChar in resultString)
            {
                if (Char.IsWhiteSpace(theChar))
                    continue;

                if (theChar == '{')
                {
                    return true;
                }
                else if (theChar == '<')
                {
                    return false;
                }
                else
                {
                    break;
                }
            }
            return false;
        }        

        public string GetDataSummary(IGridDataSummaryContext context)
        {
            if (context == null)
                return null;

            string debugFileFolder = string.Empty;
            string stylesheet = string.Empty;

            GetValues(context.Settings.Properties, context.Settings.GridDataSettings[context.GridDataName].Properties, out var connectionString, out var containerName, out var blobName, out var messageType, ref debugFileFolder, ref stylesheet, out var editXMLStylesheetInputFile);

            if (String.IsNullOrWhiteSpace(connectionString) || String.IsNullOrWhiteSpace(containerName) || String.IsNullOrWhiteSpace(blobName)) 
                return null;

            return String.Format("Bound to Azure Blob Storage CSV: ConnectionString = {0}, Container Name = {1}, Blob Name = {2}", connectionString, containerName, blobName);
        }

        internal static void GetValues(INamedSimioCollection<IAddInPropertyValue> overallSettings, INamedSimioCollection<IAddInPropertyValue> tableSettings, out string connectionString, out string containerName, out string blobName, out string messageType, ref string debugFileFolder, ref string stylesheet, out string editXMLStylesheetInputFile)
        {
            connectionString = (string)overallSettings?["ConnectionString"].Value;
            connectionString = TokenReplacement.ResolveString(connectionString, (string)overallSettings?["Password"]?.Value);
            containerName = (string)tableSettings?["ContainerName"].Value;
            blobName = (string)tableSettings?["BlobName"].Value;
            messageType = (string)tableSettings?["MessageType"].Value;
            debugFileFolder = (string)tableSettings?["DebugFileFolder"].Value;
            editXMLStylesheetInputFile = (string)tableSettings?["EditXMLStylesheetInputFile"].Value;
            stylesheet = (string)tableSettings?["Stylesheet"].Value;
        }
            
        internal static bool GetData(string connectionString, string containerName, string blobName, out string resultString)
        {
            try
            {
                var blobServiceClient = new BlobServiceClient(connectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                BlobClient blobClient = containerClient.GetBlobClient(blobName);

                var ms = new MemoryStream();
                var runStreamTask = Task.Run(() => blobClient.DownloadToAsync(ms));
                runStreamTask.Wait();
                System.Diagnostics.Trace.TraceInformation("Success Downloading Data from : " + containerName + "|" + blobName);
                resultString = Encoding.UTF8.GetString(ms.ToArray());
                return true;
            }
            catch (Exception ex) 
            {
                resultString = ex.Message;
                return false;
            }
        }

        public void Dispose()
        {
        }
    }

    class AzureBlobStorageGridDataRecords : IGridDataRecords
    {
        readonly DataSet _dataSet;

        public AzureBlobStorageGridDataRecords(DataSet dataSet)
        {
            _dataSet = dataSet;
        }

        #region IGridDataRecords Members

        List<GridDataColumnInfo> _columnInfo;
        List<GridDataColumnInfo> ColumnInfo
        {
            get
            {
                if (_columnInfo == null)
                {
                    _columnInfo = new List<GridDataColumnInfo>();

                    if (_dataSet.Tables.Count > 0)
                    {
                        foreach (DataColumn dc in _dataSet.Tables[0].Columns)
                        {
                            var name = dc.ColumnName;
                            var type = dc.DataType;

                            _columnInfo.Add(new GridDataColumnInfo()
                            {
                                Name = name,
                                Type = type
                            });
                        }
                    }
                }

                return _columnInfo;
            }
        }

        public IEnumerable<GridDataColumnInfo> Columns
        {
            get { return ColumnInfo; }
        }

        #endregion

        #region IEnumerable<IGridDataRecord> Members

        public IEnumerator<IGridDataRecord> GetEnumerator()
        {
            if (_dataSet.Tables.Count > 0)
            {
                foreach (DataRow dr in _dataSet.Tables[0].Rows)
                {
                    yield return new AzureBlobStorageGridDataRecord(dr);
                }

            }
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion    

    }

    class AzureBlobStorageGridDataRecord : IGridDataRecord
    {
        private readonly DataRow _dr;
        public AzureBlobStorageGridDataRecord(DataRow dr)
        {
            _dr = dr;
        }

        #region IGridDataRecord Members

        public string this[int index]
        {
            get
            {
                var theValue = _dr[index];

                // Simio will first try to parse dates in the current culture
                if (theValue is DateTime)
                    return ((DateTime)theValue).ToString();

                return String.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}", _dr[index]);
            }
        }

        #endregion
    }
}

