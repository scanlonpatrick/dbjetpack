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
    class SqlServer2008Writer : IDatabaseWriter    
    {
        Database _db { get; set; }
        DirectoryInfo RootDirectory { get; set; }


        public void WriteToDB(Database db, DirectoryInfo dir)
        {
            this._db = db;
            this.RootDirectory = dir;

            //* Tables
            XmlDocument tableDependancyMap = new XmlDocument();
            tableDependancyMap.LoadXml(File.ReadAllText(RootDirectory.FullName + "/Metadata/TableDependancyMap.xml"));

            List<string> tableDependanciesMet = new List<string>();
            foreach (XmlNode node in tableDependancyMap.ChildNodes[1].ChildNodes)
            {
                string objectName = node.Attributes["Name"].Value;
                foreach (string itemRun in RunTableDependanciesThenCreateObject(tableDependancyMap, tableDependanciesMet, objectName))
                    tableDependanciesMet.Add(itemRun);
            }

            /* I'm considering having the writer not look at the ignore list.  This is for 2 reasons:
             *  1. I don't want the users to wonder why the script in their directory isn't getting run
             *  2. From an implementation stantpoint, it could get sticky trying to check the ignore list before every script is run
             *  3. I don't see a need.  No records from scripted tables should refer to user-editable rows
             */

            //* Table Contents
            List<string> tableContentDependanciesMet = new List<string>();
            foreach (XmlNode node in tableDependancyMap.ChildNodes[1].ChildNodes)
            {
                string objectName = node.Attributes["Name"].Value;
                foreach (string itemRun in RunTableContentDependanciesThenInsertContents(tableDependancyMap, tableDependanciesMet, objectName))
                    tableContentDependanciesMet.Add(itemRun);
            }


            //* Everything else (functions, procs, views, etc)
            XmlDocument dependancyMap = new XmlDocument();
            dependancyMap.LoadXml(File.ReadAllText(RootDirectory.FullName + "/Metadata/DependancyMap.xml"));

            //  I don't think there can be circular dependancies.  I'm also not sure where/if they would even make sense. 
            //  Not worrying about it for now...

            List<string> dependanciesMet = new List<string>();
            foreach (XmlNode node in dependancyMap.ChildNodes[1].ChildNodes)
            {
                string objectName = node.Attributes["DBObjectName"].Value;
                string type = node.Attributes["Type"].Value;
                foreach (string itemRun in RunDependanciesThenCreateObject(dependancyMap, dependanciesMet, objectName, type))
                    dependanciesMet.Add(itemRun);
            }

            //* Indexes
            DirectoryInfo indexDir = new DirectoryInfo(RootDirectory.FullName + "\\Indexes");
            foreach (FileInfo file in indexDir.GetFiles())
            {
                string objName = file.Name.Substring(0, file.Name.Length - 4);
                Index index = null;
                
                //Table indexedTable = null;

                foreach (Table table in db.Tables)
                {
                    if (table.Indexes.Contains(objName)) index = table.Indexes[objName]; break;
                }

                string proposedDefinition = File.OpenText(file.FullName).ReadToEnd().Trim();

                if (index != null)
                {
                    //* Compare existing index definition and run if they are different
                    string currentDefinition = index.Script()[0];

                    if (!proposedDefinition.Equals(currentDefinition))
                    {
                        _db.ExecuteNonQuery(proposedDefinition.Replace("CREATE INDEX", "ALTER INDEX"));
                    }
                }
                else
                {
                    _db.ExecuteNonQuery(proposedDefinition);
                }
            }

            //* Triggers
            DirectoryInfo triggerDir = new DirectoryInfo(RootDirectory.FullName + "\\Triggers");
            foreach (FileInfo file in triggerDir.GetFiles())
            {
                string objName = file.Name.Substring(0, file.Name.Length - 4);
                Trigger trigger = null;

                //Table indexedTable = null;

                foreach (Table table in db.Tables)
                {
                    if (table.Indexes.Contains(objName)) trigger = table.Triggers[objName]; break;
                }

                string proposedDefinition = File.OpenText(file.FullName).ReadToEnd();

                if (trigger != null)
                {
                    //* Compare existing index definition and run if they are different
                    string currentDefinition = trigger.Script().ToString();

                    if (!proposedDefinition.Equals(currentDefinition))
                    {
                        _db.ExecuteNonQuery(proposedDefinition.Replace("CREATE TRIGGER", "ALTER TRIGGER"));
                    }
                }
                else
                {
                    _db.ExecuteNonQuery(proposedDefinition);
                }
            }
        }

        private List<string> RunTableContentDependanciesThenInsertContents(XmlDocument dependancyMap, List<string> dependanciesMet, string objectName)
        {
            List<string> result = new List<string>();

            //* See if there are dependancies.  If there are, check to see if they have already
            //  been run, run them if they haven't.
            XmlElement element = (XmlElement)dependancyMap.SelectSingleNode("/DependencyMap/Table[@Name=\"" + objectName + "\"]");

            //* If the xpath query returned null, this object has no dependancies; skip to the end
            if (element != null)
            {
                //* Run any dependancies not already run and capture list of what was done
                foreach (XmlNode childNode in element.ChildNodes)
                {
                    string DBObjectName = childNode.Attributes["Name"].Value;
                    if (!dependanciesMet.Contains(DBObjectName))
                    {
                        List<string> dependanciesRun = RunTableDependanciesThenCreateObject(
                                dependancyMap,
                                dependanciesMet,
                                DBObjectName
                            );

                        foreach (string item in dependanciesRun)
                            result.Add(item);
                    }
                }
            }

            StreamReader reader = new StreamReader(File.OpenRead(RootDirectory + "/TableContents/" + objectName + ".sql"));
            string proposedDefinition = reader.ReadToEnd();
            string currentDefinition = "";

            if (proposedDefinition != currentDefinition)
                _db.ExecuteNonQuery(proposedDefinition);

            //* Prepare result
            result.Add(objectName);
            return result;
        }

        private List<string> RunTableDependanciesThenCreateObject(XmlDocument dependancyMap, List<string> dependanciesMet, string objectName)
        {
            List<string> result = new List<string>();

            //* See if there are dependancies.  If there are, check to see if they have already
            //  been run, run them if they haven't.
            XmlElement element = (XmlElement)dependancyMap.SelectSingleNode("/DependencyMap/Table[@Name=\"" + objectName + "\"]");

            //* If the xpath query returned null, this object has no dependancies; skip to the end
            if (element != null)
            {
                //* Run any dependancies not already run and capture list of what was done
                foreach (XmlNode childNode in element.ChildNodes)
                {
                    string DBObjectName = childNode.Attributes["Name"].Value;
                    if (!dependanciesMet.Contains(DBObjectName))
                    {
                        List<string> dependanciesRun = RunTableDependanciesThenCreateObject(
                                dependancyMap,
                                dependanciesMet,
                                DBObjectName
                            );

                        foreach (string item in dependanciesRun)
                            result.Add(item);
                    }
                }
            }

            StreamReader reader = new StreamReader(File.OpenRead(RootDirectory + "/Tables/" + objectName + ".sql"));
            string proposedDefinition = reader.ReadToEnd();
            string currentDefinition = "";

            if (proposedDefinition != currentDefinition)
                _db.ExecuteNonQuery(proposedDefinition);

            //* Prepare result
            result.Add(objectName);
            return result;
        }

        private List<string> RunDependanciesThenCreateObject(XmlDocument dependancyMap, List<string> dependanciesMet, string objectName, string objectType)
        {
            List<string> result = new List<string>();

            //* See if there are dependancies.  If there are, check to see if they have already
            //  been run, run them if they haven't.
            XmlElement element = (XmlElement)dependancyMap.SelectSingleNode("/DependancyMap/DBObject[@DBObjectName=\"" + objectName + "\"]");

            //* If the xpath query returned null, this object has no dependancies; skip to the end
            if (element != null)
            {
                //* Run any dependancies not already run and capture list of what was done
                foreach (XmlNode childNode in element.ChildNodes)
                {
                    string DBObjectName = childNode.Attributes["DBObjectName"].Value;
                    string childObjectType = childNode.Attributes["Type"].Value;

                    if (!dependanciesMet.Contains(DBObjectName))
                    {
                        List<string> dependanciesRun = RunDependanciesThenCreateObject(
                                dependancyMap,
                                dependanciesMet,
                                DBObjectName,
                                childObjectType
                            );

                        foreach (string item in dependanciesRun)
                            result.Add(item);
                    }
                }
            }

            //* At this point either all dependancies have been met or there aren't any
            //  so create the object
            RunDefinition(objectName, StringToDBObjectType(objectType.Trim()));

            //* Prepare result
            result.Add(objectName);
            return result;
        }

        private DatabaseObjectTypes StringToDBObjectType(string type)
        {
            //* this will probably be a work in progress for a while...
            switch (type)
            {
                case "P": return DatabaseObjectTypes.StoredProcedure; break;
                case "FN": return DatabaseObjectTypes.UserDefinedFunction; break;
                case "V": return DatabaseObjectTypes.View; break;
                default: throw new ArgumentException("invalid object type code", "type");
            }
        }

        private void RunDefinition(string objName, DatabaseObjectTypes objType)
        {
            StreamReader reader = new StreamReader(File.OpenRead(RootDirectory + "/" + objType.ToString() + "/" + objName + ".sql"));
            string proposedDefinition = reader.ReadToEnd();
            string currentDefinition = "";
            string typeName = "";

            switch (objType)
            {
                case DatabaseObjectTypes.StoredProcedure:
                    if (_db.StoredProcedures.Contains(objName))
                        currentDefinition = _db.StoredProcedures[objName].Script().ToString();

                    typeName = "PROCEDURE";
                    break;
                case DatabaseObjectTypes.UserDefinedFunction:
                    if (_db.UserDefinedFunctions.Contains(objName))
                        currentDefinition = _db.UserDefinedFunctions[objName].Script().ToString();

                    typeName = "FUNCTION";
                    break;
                case DatabaseObjectTypes.View:
                    if (_db.Views.Contains(objName))
                        currentDefinition = _db.Views[objName].Script().ToString();

                    typeName = "VIEW";
                    break;
                default: throw new ApplicationException("The application attempted to run a definition for an unsupported database object type");
            }

            if (!string.IsNullOrEmpty(currentDefinition))
            {
                if (!proposedDefinition.Equals(currentDefinition))
                {
                    _db.ExecuteNonQuery(proposedDefinition.Replace("CREATE " + typeName, "ALTER " + typeName));
                }
            }
            else
                _db.ExecuteNonQuery(proposedDefinition);
        }
    }
}
