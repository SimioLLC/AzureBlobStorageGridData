using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SimioAPI;
using SimioAPI.Extensions;
using System;
using System.IO;    
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using System.Runtime.InteropServices.ComTypes;
using System.Data;
using System.Globalization;
using System.Xml;
using System.Xml.Linq;

namespace AzureBlobStorageGridData
{
    public class ExporterDefinition : IGridDataExporterDefinition
    {
        public string Name => "Azure Blob Storage Data Exporter";
        public string Description => "An exporter to Azure Blob Storage formatted data";
        public Image Icon => null;

        static readonly Guid MY_ID = new Guid("c097b3de-6007-4c11-8f56-4eb089e71e9e");
        public Guid UniqueID => MY_ID;

        public IGridDataExporter CreateInstance(IGridDataExporterContext context)
        {
            return new Exporter(context);
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

            var editXMLStylesheetInputFile = schema.PerTableProperties.AddFileProperty("EditXMLStylesheetInputFile");
            editXMLStylesheetInputFile.DisplayName = "Edit XML Stylesheet Input File";
            editXMLStylesheetInputFile.Description = "Edit XML Stylesheet Input File.";
            editXMLStylesheetInputFile.DefaultValue = String.Empty;
        }
        private void StylesheetProp_GetXML(object sender, XSLTAddInPropertyGetXMLEventArgs e)
        {
            string stylesheet = string.Empty;

            Exporter.GetValues(e.HierarchicalProperties[0], e.OtherProperties, out var connectionString, out var containerName, out var blobName, out var messageType, ref stylesheet, out var editXMLStylesheetInputFile);

            Console.WriteLine("\nReading blob from\n\t{0}\n", editXMLStylesheetInputFile);

            e.XML = File.ReadAllText(editXMLStylesheetInputFile);
        }
    }

    class Exporter : IGridDataExporter
    {
        public Exporter(IGridDataExporterContext context)
        {
        }

        public OpenExportDataResult OpenData(IGridDataOpenExportDataContext openContext)
        {
            string stylesheet = string.Empty;

            GetValues(openContext.Settings.Properties, openContext.Settings.GridDataSettings[openContext.GridDataName].Properties, out var connectionString, out var containerName, out var blobName, out var messageType, ref stylesheet, out var editXMLStylesheetInputFile);

            if (String.IsNullOrWhiteSpace(connectionString))
                return OpenExportDataResult.Failed("The Connection String parameter is not specified");

            if (String.IsNullOrWhiteSpace(containerName))
                return OpenExportDataResult.Failed("The Container Name parameter is not specified");

            if (String.IsNullOrWhiteSpace(blobName))
                return OpenExportDataResult.Failed("The Blob Name parameter is not specified");

            if (messageType != "JSON" && messageType != "XML" && messageType != "OTHER")
                return OpenExportDataResult.Failed("Invalid Message Type");

            var dataTable = ConvertExportRecordsToDataTable(openContext.Records, openContext.GridDataName);
            DataSet dataSet = new DataSet();
            dataSet.Tables.Add(dataTable);
            var xmlString = dataSet.GetXml();

            var result = Simio.Xml.XsltTransform.TransformXmlToDataSet(xmlString, stylesheet, null, out var finalXMLString);

            if (messageType == "JSON")
            {
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.LoadXml(finalXMLString.Replace("\r\n", String.Empty).Trim());
                XmlElement root = xmlDoc.DocumentElement;
                finalXMLString = JsonConvert.SerializeXmlNode(xmlDoc.FirstChild);
            }     

            using (var ms = new System.IO.MemoryStream())
            using (var sw = new System.IO.StreamWriter(ms))
            {
                //dataSet.WriteXml(sw);
                sw.WriteLine(finalXMLString);
                sw.Flush();
                ms.Position = 0;

                var blobServiceClient = new BlobServiceClient(connectionString);

                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                BlobClient blobClient = containerClient.GetBlobClient(blobName);

                // Upload data from the stream file, overwrite the blob if it already exists
                var runUploadTask = Task.Run(() => blobClient.UploadAsync(ms, true));
                runUploadTask.Wait();

                System.Diagnostics.Trace.TraceInformation("Success Exporting Data to : " + containerName + "|" + blobName);
            }

            return OpenExportDataResult.Succeeded();
        }            

        public string GetDataSummary(IGridDataSummaryContext context)
        {
            if (context == null)
                return null;

              string stylesheet = string.Empty;

            GetValues(context.Settings.Properties, context.Settings.GridDataSettings[context.GridDataName].Properties, out var connectionString, out var containerName, out var blobName, out var messageType, ref stylesheet, out var editXMLStylesheetInputFile);

            if (String.IsNullOrWhiteSpace(connectionString) || String.IsNullOrWhiteSpace(containerName) || String.IsNullOrWhiteSpace(blobName))
                return null;

            return String.Format("Exporting to Azure Blob Storage CSV: Connection String = {0}, Container Name = {1}, Blob Name = {2}", connectionString, containerName, blobName);
        }

        internal static void GetValues(INamedSimioCollection<IAddInPropertyValue> overallSettings, INamedSimioCollection<IAddInPropertyValue> tableSettings, out string connectionString, out string containerName, out string blobName, out string messageType, ref string stylesheet, out string editXMLStylesheetInputFile)
        {
            connectionString = (string)overallSettings?["ConnectionString"].Value;
            connectionString = TokenReplacement.ResolveString(connectionString, (string)overallSettings?["Password"]?.Value);
            containerName = (string)tableSettings?["ContainerName"].Value;
            blobName = (string)tableSettings?["BlobName"].Value;
            messageType = (string)tableSettings?["MessageType"].Value;
            stylesheet = (string)tableSettings?["Stylesheet"].Value;
            editXMLStylesheetInputFile = (string)tableSettings?["EditXMLStylesheetInputFile"].Value;
        }

        public void Dispose()
        {
        }

        internal static DataTable ConvertExportRecordsToDataTable(IGridDataExportRecords exportRecord, string tableName)
        {
            // New table
            var dataTable = new DataTable();
            dataTable.TableName = tableName;
            dataTable.Locale = CultureInfo.InvariantCulture;

            List<IGridDataExportColumnInfo> colImportLocalRecordsColumnInfoList = new List<IGridDataExportColumnInfo>();

            foreach (var col in exportRecord.Columns)
            {
                colImportLocalRecordsColumnInfoList.Add(col);
                var dtCol = dataTable.Columns.Add(col.Name, Nullable.GetUnderlyingType(col.Type) ?? col.Type);
            }

            // Add Rows to data table
            foreach (var record in exportRecord)
            {
                object[] thisRow = new object[dataTable.Columns.Count];

                int dbColIndex = 0;
                foreach (var colExportLocalRecordsColumnInfo in colImportLocalRecordsColumnInfoList)
                {
                    var valueObj = record.GetNativeObject(dbColIndex);
                    thisRow[dbColIndex] = valueObj;
                    dbColIndex++;
                }

                dataTable.Rows.Add(thisRow);
            }

            return dataTable;
        }
    }
}
