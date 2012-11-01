using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using System.Collections.Specialized;
using System.Data.SqlClient;
using System.IO;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using System.Xml;
using System.Data;

namespace DBJetpack.DBManagers
{
    class SqlServer2008Reader : IDatabaseReader
    {
        Database Database { get; set; }
        DirectoryInfo RootDirectory { get; set; }

        public void ReadFromDB(Database db, DirectoryInfo dir)
        {
            this.Database = db;
            this.RootDirectory = dir;

            //* Stored Procedures
            DirectoryInfo procDirectory = RootDirectory.CreateSubdirectory(DatabaseObjectTypes.StoredProcedure.ToString());

            foreach (StoredProcedure proc in Database.StoredProcedures)
            {
                if (!proc.IsSystemObject)
                {
                    FileStream file = File.Create(procDirectory.FullName + "/" + proc.Name + ".sql");
                    StreamWriter writer = new StreamWriter(file);

                    ScriptingOptions so = new ScriptingOptions();
                    so.ScriptBatchTerminator = true;

                    try
                    {
                        foreach (string line in proc.Script(so))
                        {
                            //* Kludge, c.f. http://dbaspot.com/sqlserver-programming/421701-smo-scripting-set-commands.html
                            if (line.Contains("CREATE PROCEDURE")) writer.WriteLine("GO");
                            writer.WriteLine(line);
                        }

                        writer.Flush();
                    }
                    finally
                    {
                        writer.Close();
                    }

                }
            }

            //* Functions
            DirectoryInfo functionDirectory = RootDirectory.CreateSubdirectory(DatabaseObjectTypes.UserDefinedFunction.ToString());

            foreach (UserDefinedFunction function in Database.UserDefinedFunctions)
            {
                if (!function.IsSystemObject)
                {
                    FileStream file = File.Create(functionDirectory.FullName + "/" + function.Name + ".sql");
                    StreamWriter writer = new StreamWriter(file);

                    try
                    {
                        foreach (string line in function.Script())
                        {
                            //* Kludge, c.f. http://dbaspot.com/sqlserver-programming/421701-smo-scripting-set-commands.html
                            if (line.Contains("CREATE FUNCTION")) writer.WriteLine("GO");
                            writer.WriteLine(line);
                        }

                        writer.Flush();
                    }
                    finally
                    {
                        writer.Close();
                    }
                }
            }

            //* Triggers
            DirectoryInfo triggerDirectory = RootDirectory.CreateSubdirectory("Triggers");

            foreach (Trigger trigger in Database.Triggers)
            {
                if (!trigger.IsSystemObject)
                {
                    FileStream file = File.Create(triggerDirectory.FullName + "/" + trigger.Name + ".sql");
                    StreamWriter writer = new StreamWriter(file);

                    try
                    {
                        foreach (string line in trigger.Script())
                        {
                            //* Kludge, c.f. http://dbaspot.com/sqlserver-programming/421701-smo-scripting-set-commands.html
                            if (line.Contains("CREATE TRIGGER")) writer.WriteLine("GO");
                            writer.WriteLine(line);
                        }

                        writer.Flush();
                    }
                    finally
                    {
                        writer.Close();
                    }
                }
            }

            //* Indexes
            DirectoryInfo indexesDirectory = RootDirectory.CreateSubdirectory("Indexes");

            foreach (Table table in Database.Tables)
            {
                if (!table.IsSystemObject)
                {
                    foreach (Index index in table.Indexes)
                    {
                        FileStream file = File.Create(indexesDirectory.FullName + "/" + index.Name + ".sql");
                        StreamWriter writer = new StreamWriter(file);

                        try
                        {
                            foreach (string line in index.Script())
                            {
                                //* Kludge, c.f. http://dbaspot.com/sqlserver-programming/421701-smo-scripting-set-commands.html
                                if (line.Contains("CREATE INDEX")) writer.WriteLine("GO");
                                writer.WriteLine(line);
                            }

                            writer.Flush();
                        }
                        finally
                        {
                            writer.Close();
                        }
                    }

                }
            }

            //* Tables
            DirectoryInfo tableDir = RootDirectory.CreateSubdirectory("Tables");
            foreach (Table table in Database.Tables)
            {
                StringBuilder script = new StringBuilder();
                script.Append("IF NOT EXISTS ")
                .Append("(SELECT * FROM SYS.TABLES T JOIN SYS.SCHEMAS S ")
                .Append("ON T.SCHEMA_ID = S.SCHEMA_ID ")
                .Append("WHERE T.NAME = '" + table.Name + "' AND S.NAME = '" + table.Schema + "')\r\n")
                .Append("BEGIN\r\n")
                .Append("\tCREATE TABLE [" + table.Name + "] ( \r\n");

                bool hasPrimaryKey = false;
                foreach (Index index in table.Indexes) if (index.IndexKeyType == IndexKeyType.DriPrimaryKey) hasPrimaryKey = true;

                if (hasPrimaryKey)
                {
                    string indexDefinitions = "";
                    //* CREATE TABLE script will have only the primary key(s)
                    foreach (Index index in table.Indexes)
                    {
                        if (index.IndexKeyType == IndexKeyType.DriPrimaryKey)
                        {
                            foreach (IndexedColumn column in index.IndexedColumns)
                            {
                                if (!string.IsNullOrEmpty(indexDefinitions)) indexDefinitions += ", \r\n";
                                indexDefinitions += column.Name + " " + table.Columns[column.Name].DataType.Name;

                                if (table.Columns[column.Name].DataType.SqlDataType == SqlDataType.VarChar
                                    || table.Columns[column.Name].DataType.SqlDataType == SqlDataType.NVarChar)
                                    indexDefinitions += " (" + table.Columns[column.Name].DataType.MaximumLength + ")";

                                indexDefinitions += " NOT NULL";

                            }
                        }
                    }

                    script.Append("\t" + indexDefinitions + "\r\n");
                    script.Append("\t)\r\n END  \r\n GO");

                    //* Append the rest of the columns as ALTER TABLE statements
                    foreach (Column column in table.Columns)
                    {
                        script.Append("\r\n \r\n")
                            .Append("IF NOT EXISTS (SELECT * FROM SYS.COLUMNS WHERE NAME = '" + column.Name + "' AND Object_ID = Object_ID('" + table.Name + "'))")
                            .Append("\r\n\tALTER TABLE [" + table.Name + "] add [" + column.Name + "] [" + column.DataType.ToString() + "] ")
                            .Append((column.DataType.SqlDataType == SqlDataType.VarChar || column.DataType.SqlDataType == SqlDataType.NVarChar) ? "(" + column.DataType.MaximumLength.ToString() + ")" : "")
                            .Append(column.Nullable ? " NULL" : " NOT NULL")
                            .Append("\r\nGO\r\n");

                        //* Append alter statement if column type is a varchar
                        if (column.DataType.SqlDataType == SqlDataType.VarChar || column.DataType.SqlDataType == SqlDataType.NVarChar)
                            script.Append("\r\nIF (SELECT max_length FROM SYS.COLUMNS WHERE NAME = '" + column.Name + "' AND Object_ID = Object_ID('" + table.Name + "')) <> " + column.DataType.MaximumLength.ToString())
                                .Append("\r\n\tALTER TABLE [" + table.Name + "] ALTER COLUMN [" + column.Name + "] " + column.DataType.ToString())
                                .Append("(" + column.DataType.MaximumLength.ToString() + ")")
                                .Append(column.Nullable ? " NULL" : " NOT NULL");
                    }
                }
                else
                {
                    //* there is no primary key so create the table with just the first column, then check for each column and
                    //  add it if it doesn't already exist

                    script.Append("\t" + table.Columns[0].Name + " " + table.Columns[table.Columns[0].Name].DataType.Name);

                    if (table.Columns[table.Columns[0].Name].DataType.SqlDataType == SqlDataType.VarChar
                        || table.Columns[table.Columns[0].Name].DataType.SqlDataType == SqlDataType.NVarChar)
                        script.Append(" (" + table.Columns[table.Columns[0].Name].DataType.MaximumLength + ")" + "\r\n");

                    script.Append("\t)\r\n END  \r\n GO");

                    //* Append the rest of the columns as ALTER TABLE statements
                    foreach (Column column in table.Columns)
                    {
                        script.Append("\r\n \r\n")
                            .Append("IF NOT EXISTS (SELECT * FROM SYS.COLUMNS WHERE NAME = '" + column.Name + "' AND Object_ID = Object_ID('" + table.Name + "'))")
                            .Append("\r\n\tALTER TABLE [" + table.Name + "] add [" + column.Name + "] [" + column.DataType.ToString() + "] ")
                            .Append((column.DataType.SqlDataType == SqlDataType.VarChar || column.DataType.SqlDataType == SqlDataType.NVarChar) ? "(" + column.DataType.MaximumLength.ToString() + ")" : "")
                            .Append(column.Nullable ? " NULL" : " NOT NULL")
                            .Append("\r\nGO\r\n");

                        //* Append alter statement if column type is a varchar
                        if (column.DataType.SqlDataType == SqlDataType.VarChar || column.DataType.SqlDataType == SqlDataType.NVarChar)
                            script.Append("\r\nIF (SELECT max_length FROM SYS.COLUMNS WHERE NAME = '" + column.Name + "' AND Object_ID = Object_ID('" + table.Name + "')) <> " + column.DataType.MaximumLength.ToString())
                                .Append("\r\n\tALTER TABLE [" + table.Name + "] ALTER COLUMN [" + column.Name + "] " + column.DataType.ToString())
                                .Append("(" + column.DataType.MaximumLength.ToString() + ")")
                                .Append(column.Nullable ? " NULL" : " NOT NULL");
                    }
                }

                FileStream file = File.Create(tableDir.FullName + "/" + table.Name + ".sql");
                StreamWriter writer = new StreamWriter(file);

                try
                {
                    writer.WriteLine(script);
                    writer.Flush();
                }
                finally
                {
                    writer.Close();
                }
            }

            //* Unpack ignore list
            List<string> ignoreList = new List<string>();
            if (File.Exists(RootDirectory.FullName + "\\Metadata\\IgnoreList.txt"))
            {
                StreamReader reader = new StreamReader(File.OpenRead(RootDirectory.FullName + "\\Metadata\\IgnoreList.txt"));
                string currentLine = reader.ReadLine();

                while (currentLine != null)
                {
                    if (!currentLine.StartsWith("#"))
                        ignoreList.Add(currentLine);

                    currentLine = reader.ReadLine();
                }
            }

            //* Table contents
            DirectoryInfo tableContentsDir = RootDirectory.CreateSubdirectory("TableContents");
            foreach (Table table in Database.Tables)
            {
                if (!ignoreList.Contains(table.Name))
                {
                    //* Declare table variable
                    string script = "";

                    if (table.RowCount > 0)
                    {
                        foreach (Column column in table.Columns)
                        {
                            if (string.IsNullOrEmpty(script)) script += "DECLARE @TABLE TABLE("; else script += ",\r\n";
                            script += "\t[" + column.Name + "] " + column.DataType;
                            if (column.DataType.SqlDataType == SqlDataType.VarChar || column.DataType.SqlDataType == SqlDataType.NVarChar) script += "(" + column.DataType.MaximumLength.ToString() + ")";
                        }

                        script += "\r\n)\r\n";
                        script += "INSERT INTO @TABLE \r\n";

                        string columnList = "";
                        foreach (Column column in table.Columns)
                        {
                            if (!string.IsNullOrEmpty(columnList)) columnList += ", ";
                            columnList += "[" + column.Name + "]";
                        }

                        //* Get table contents and append to script
                        DataSet dataset = Database.ExecuteWithResults("select " + columnList + " from [" + table.Name + "]");
                        foreach (DataRow row in dataset.Tables[0].Rows)
                        {
                            string rowData = "";
                            foreach (Column column in table.Columns)
                            {
                                if (string.IsNullOrEmpty(rowData)) rowData += "\r\n SELECT "; else rowData += ", ";

                                if (column.DataType.SqlDataType == SqlDataType.VarChar
                                    || column.DataType.SqlDataType == SqlDataType.NVarChar
                                    || column.DataType.SqlDataType == SqlDataType.UniqueIdentifier)
                                    rowData += "'";

                                if (column.DataType.SqlDataType == SqlDataType.Bit)
                                {
                                    if (bool.Parse(row[column.Name].ToString()))
                                        rowData += "1";
                                    else
                                        rowData += "0";
                                }
                                else
                                    rowData += row[column.Name].ToString();

                                if (column.DataType.SqlDataType == SqlDataType.VarChar
                                    || column.DataType.SqlDataType == SqlDataType.NVarChar
                                    || column.DataType.SqlDataType == SqlDataType.UniqueIdentifier)
                                    rowData += "'";
                            }
                            script += rowData + "\r\n";

                            //* Append UNION ALL as long as this is not the last row
                            if (dataset.Tables[0].Rows.IndexOf(row) != dataset.Tables[0].Rows.Count - 1) script += "\r\n UNION ALL";
                        }

                        script += "\r\n\r\n";

                        script += "MERGE [" + table.Name + "] as target\r\n";
                        script += "USING @Table as source\r\n";
                        script += "ON ";

                        //* If there is a primary key, use it; if not, just make sure the record is there.
                        string onClause = "";
                        foreach (Index index in table.Indexes)
                        {
                            if (index.IndexKeyType == IndexKeyType.DriPrimaryKey)
                            {
                                foreach (IndexedColumn column in index.IndexedColumns)
                                {
                                    if (!string.IsNullOrEmpty(onClause)) onClause += " and ";
                                    onClause += "target.[" + column.Name + "] = source.[" + column.Name + "]";
                                }
                            }
                        }

                        //* If there is no primary key attempt to match ALL columns 
                        //  NB: of course if we go this route and there is a match, it won't do anything since the row already
                        //  exists.  This will be in place only if there is no match, so we can ensure that the
                        //  record is created.
                        if (string.IsNullOrEmpty(onClause))
                        {
                            foreach (Column column in table.Columns)
                            {
                                if (!string.IsNullOrEmpty(onClause)) onClause += ", ";
                                onClause += "target.[" + column.Name + "] = source.[" + column.Name + "]";
                            }
                        }

                        script += onClause + "\r\n";

                        script += "WHEN MATCHED THEN UPDATE SET\r\n";

                        string matchedClause = "";
                        foreach (Column column in table.Columns)
                        {
                            if (!string.IsNullOrEmpty(matchedClause)) matchedClause += ", \r\n";
                            matchedClause += "target.[" + column.Name + "] = source.[" + column.Name + "]";
                        }

                        script += matchedClause + "\r\n";

                        script += "WHEN NOT MATCHED BY target THEN\r\n";
                        script += "INSERT (";

                        string notMatchedTargetClause = "";
                        string notMatchedTargetClauseValues = "";
                        foreach (Column column in table.Columns)
                        {
                            if (!string.IsNullOrEmpty(notMatchedTargetClause)) notMatchedTargetClause += ", ";
                            notMatchedTargetClause += column.Name;

                            if (!string.IsNullOrEmpty(notMatchedTargetClauseValues)) notMatchedTargetClauseValues += ", ";
                            notMatchedTargetClauseValues += "source.[" + column.Name + "]";
                        }

                        script += notMatchedTargetClause + ") \r\n";
                        script += "VALUES (" + notMatchedTargetClauseValues + ")";

                        //* TODO: Only delete if the Cleanup flag is set to true; cleanup flag doesn't exist yet...
                        //  script += "WHEN NOT MATCHED BY SOURCE THEN DELETE";

                        //* Merge statements must always end with a semicolon
                        script += ";";
                    }
                    else
                    {
                        //* TODO: If the Cleanup option is specified, delete all rows from table
                        script = "-- This table has no records";
                    }

                    FileStream file = File.Create(tableContentsDir.FullName + "/" + table.Name + ".sql");
                    StreamWriter writer = new StreamWriter(file);

                    try
                    {
                        writer.WriteLine(script);
                        writer.Flush();
                    }
                    finally
                    {
                        writer.Close();
                    }
                }
                else
                {
                    //* this table is on the ignore list; create the file but only add a comment
                    StreamWriter writer = File.CreateText(tableContentsDir.FullName + "/" + table.Name + ".sql");

                    try
                    {
                        writer.WriteLine("--* This table was on the ignore list and therefore was not scripted.");
                        writer.Flush();
                    }
                    finally
                    {
                        writer.Close();
                    }
                }
            }

            //* Table Dependancy Map
            DirectoryInfo metadataDir = RootDirectory.CreateSubdirectory("Metadata");
            XmlWriter tableXmlWriter = XmlWriter.Create(File.Create(metadataDir.FullName + "/TableDependancyMap.xml"));

            XmlDocument tableDependancyMap = GetTableDependancyMap();

            try
            {
                tableDependancyMap.WriteTo(tableXmlWriter);
                tableXmlWriter.Flush();
            }
            finally
            {
                tableXmlWriter.Close();
            }

            //* Dependancy Map
            XmlWriter xmlWriter = XmlWriter.Create(File.Create(metadataDir.FullName + "/DependancyMap.xml"));

            XmlDocument dependancyMap = GetDependancyMap();

            try
            {
                dependancyMap.WriteTo(xmlWriter);
                xmlWriter.Flush();
            }
            finally
            {
                xmlWriter.Close();
            }


        }

        private XmlDocument GetTableDependancyMap()
        {
            XmlDocument dependancyMap = new XmlDocument();
            dependancyMap.AppendChild(dependancyMap.CreateProcessingInstruction("xml", "version=\"1.0\" encoding=\"UTF-8\""));
            XmlElement rootElement = dependancyMap.CreateElement("DependancyMap");
            dependancyMap.AppendChild(rootElement);

            foreach (Table table in Database.Tables)
            {
                //* Query for the xml element, create it if it doesn't already exist
                //  NB:  this page has a lot of good Xml manipulation info: http://omegacoder.com/?p=46
                XmlElement element = (XmlElement)dependancyMap.SelectSingleNode("/DependencyMap/Table[@Name=\"" + table.Name + "\"]");

                if (element == null)
                {
                    element = dependancyMap.CreateElement("Table");
                    element.SetAttribute("Name", table.Name);
                    rootElement.AppendChild(element);
                }

                foreach (ForeignKey foreignKey in table.ForeignKeys)
                {
                    //* Add dependency
                    XmlElement dependencyElement = dependancyMap.CreateElement("Dependency");
                    dependencyElement.SetAttribute("Name", foreignKey.ReferencedTable);
                    element.AppendChild(dependencyElement);
                }
            }

            return dependancyMap;
        }

        private XmlDocument GetDependancyMap()
        {
            XmlDocument dependancyMap = new XmlDocument();
            dependancyMap.AppendChild(dependancyMap.CreateProcessingInstruction("xml", "version=\"1.0\" encoding=\"UTF-8\""));
            XmlElement rootElement = dependancyMap.CreateElement("DependancyMap");
            dependancyMap.AppendChild(rootElement);

            DataSet dataset = Database.ExecuteWithResults(@"
select 	
	s.name as dependantSchema,
	base.name as dependantName, 
	base.type as dependantType,
	d.referenced_schema_name as dependencySchema, 
	d.referenced_entity_name as dependencyName, 
	dependancy.type as dependancyType
from sys.sql_expression_dependencies d
join sys.objects base on d.referencing_id = base.[object_id]
join sys.schemas s on base.[schema_id] = s.[schema_id]
join sys.objects dependancy on d.referenced_id = dependancy.[object_id]
where dependancy.type <> 'U'
");

            //* There should be at least 1 table...
            DataTable table = dataset.Tables[0];

            //* Loop through each row and add it to the appropriate XML element (creating it if necessary)
            foreach (DataRow row in table.Rows)
            {
                //* Extract information from row
                string dependantSchema = Convert.ToString(row["dependantSchema"]);
                string dependantName = Convert.ToString(row["dependantName"]);
                string dependantType = Convert.ToString(row["dependantType"]);
                string dependencySchema = Convert.ToString(row["dependencySchema"]);
                string dependencyName = Convert.ToString(row["dependencyName"]);
                string dependancyType = Convert.ToString(row["dependancyType"]);

                //* Query for the xml element, create it if it doesn't already exist
                //  NB:  this page has a lot of good Xml manipulation info: http://omegacoder.com/?p=46
                XmlElement element = (XmlElement)dependancyMap.SelectSingleNode("/DependencyMap/DBObject[@name=\"" + dependantName + "\"]");

                if (element == null)
                {
                    element = dependancyMap.CreateElement("DBObject");
                    element.SetAttribute("DBObjectName", dependantName);
                    element.SetAttribute("Type", dependantType);
                    rootElement.AppendChild(element);
                }

                //* Add dependency
                XmlElement dependencyElement = dependancyMap.CreateElement("Dependency");
                dependencyElement.SetAttribute("DBObjectName", dependencyName);
                dependencyElement.SetAttribute("Type", dependancyType);

                element.AppendChild(dependencyElement);
            }
            return dependancyMap;
        }

    }
}
