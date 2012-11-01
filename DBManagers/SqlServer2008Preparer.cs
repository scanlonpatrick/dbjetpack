using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Microsoft.SqlServer.Management.Smo;

namespace DBJetpack.DBManagers
{
    class SqlServer2008Preparer: IDatabasePreparer
    {
        public void Prep(Microsoft.SqlServer.Management.Smo.Database db, System.IO.DirectoryInfo dir)
        {
            DirectoryInfo metadataDir = dir.CreateSubdirectory("Metadata");
            string path = metadataDir.FullName + "/" + "IgnoreList.txt";

            //* If the file already exists, leave the settings as they are and append any tables that are not already there
            Dictionary<string, bool> existingTables = new Dictionary<string,bool>();
            StreamWriter writer = null;

            if (File.Exists(path))
            {
                using (StreamReader reader = new StreamReader(File.OpenRead(path)))
                {
                    

                    while (!reader.EndOfStream)
                    {
                        string currentLine = reader.ReadLine();
                        if (!string.IsNullOrEmpty(currentLine))
                        {
                            if (currentLine.StartsWith("#"))
                                existingTables.Add(currentLine.Substring(1), false);
                            else
                                existingTables.Add(currentLine, true);
                        }
                    }
                }

                writer = new StreamWriter(File.OpenWrite(path));
            }
            else
                writer = File.CreateText(path);

            //* Tables
            writer.WriteLine();
            writer.WriteLine("#*** Tables");
            writer.WriteLine();
            foreach(Table table in db.Tables)
            {
                if (!table.IsSystemObject)
                {
                    writer.Write(existingTables.Keys.Contains<string>(table.Name)
                        && existingTables[table.Name]
                        ? ""
                        : "#");
                    writer.WriteLine(table.Name + "\r\n");
                }
            }

            //* Views
            writer.WriteLine();
            writer.WriteLine("#*** Views");
            writer.WriteLine();
            foreach (View view in db.Views)
            {
                if (!view.IsSystemObject)
                {
                    writer.Write(existingTables.Keys.Contains<string>(view.Name)
                        && existingTables[view.Name]
                        ? ""
                        : "#");
                    writer.WriteLine(view.Name + "\r\n");
                }
            }

            //* Functions
            writer.WriteLine();
            writer.WriteLine("#*** Functions");
            writer.WriteLine();
            foreach (UserDefinedFunction func in db.UserDefinedFunctions)
            {
                if (!func.IsSystemObject)
                {
                    writer.Write(existingTables.Keys.Contains<string>(func.Name)
                        && existingTables[func.Name]
                        ? ""
                        : "#");
                    writer.WriteLine(func.Name + "\r\n");
                }
            }

            //* Stored Procs
            writer.WriteLine();
            writer.WriteLine("#*** Stored Procedures");
            writer.WriteLine();
            foreach (StoredProcedure proc in db.StoredProcedures)
            {
                if (!proc.IsSystemObject)
                {
                    writer.Write(existingTables.Keys.Contains<string>(proc.Name)
                        && existingTables[proc.Name]
                        ? ""
                        : "#");
                    writer.WriteLine(proc.Name + "\r\n");
                }
            }

            //* Indexes
            writer.WriteLine();
            writer.WriteLine("#*** Indexes");
            writer.WriteLine();
            foreach (Table table in db.Tables)
            {
                if (!table.IsSystemObject)
                {
                    foreach (Index index in table.Indexes)
                    {
                        writer.Write(existingTables.Keys.Contains<string>(index.Name)
                            && existingTables[index.Name]
                            ? ""
                            : "#");
                        writer.WriteLine(index.Name + "\r\n");
                    }

                }
            }

            //* Triggers
            writer.WriteLine();
            writer.WriteLine("#*** Triggers");
            writer.WriteLine();
            foreach (Trigger trigger in db.Triggers)
            {
                if (!trigger.IsSystemObject)
                {
                    writer.Write(existingTables.Keys.Contains<string>(trigger.Name)
                        && existingTables[trigger.Name]
                        ? ""
                        : "#");
                }
            }

            try
            {
                writer.Flush();
            }
            finally
            {
                writer.Close();
            }
        }
    }
}
