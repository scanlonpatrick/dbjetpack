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

namespace BumbleBee
{
    class Program
    {
        private static DirectoryInfo RootDirectory { get; set; }
        private static Database Database { get; set; }

        static void Main(string[] args)
        {
            /* commands used:
             * prep -conn="Data Source=winvm;Initial Catalog= TEST_DB;Integrated Security=SSPI;" -scriptFileRoot="." -dbName="TEST_DB"
             * read -conn="Data Source=winvm;Initial Catalog= TEST_DB;Integrated Security=SSPI;" -scriptFileRoot="." -dbName="TEST_DB"
             */
            string conn = "";
            string configFilePath = "";
            bool cleanUp;
            string dbName = "";
            string scriptFileRoot = "";
            bool dryRun;
            Mode mode = Mode.None;

            //* parse args
            //  the first arg should be either "read" or "write" (i.e. the "mode")
            if (!(args[0] == "read" || args[0] == "write" || args[0] == "prep")) throw new ApplicationException("the first argument to BumbleBee must be \"read\" or \"write\"");

            foreach (string arg in args)
            {
                if (arg.StartsWith("-conn"))
                    conn = arg.Substring(6);
                else if (arg == "-cleanUp")
                    cleanUp = true;
                else if (arg == "-dryRun")
                    dryRun = true;
                else if (arg.StartsWith("-dbName"))
                    dbName = arg.Substring(8);
                else if (arg.StartsWith("-configFilePath"))
                    configFilePath = arg.Substring(16);
                else if (arg.StartsWith("-scriptFileRoot"))
                    scriptFileRoot = arg.Substring(16);
                else if (arg == "read")
                    mode = Mode.Read;
                else if (arg == "write")
                    mode = Mode.Write;
                else if (arg == "prep")
                    mode = Mode.Prep;
                else
                    throw new ApplicationException("Unrecognized argument: " + arg);
            }

            //* If a config file is specified AND command line args are given, 
            //  command line args take precedent.
            if (!string.IsNullOrEmpty(configFilePath))
            {
                //* TODO: read config file
            }

            SqlConnection connection = new SqlConnection();
            //connection.ConnectionString = "Data Source=eudaimonia\\sqlexpress;Initial Catalog= A_DB;Integrated Security=SSPI;";
            connection.ConnectionString = conn;

            Server server = new Server(new ServerConnection(connection));

            if (string.IsNullOrEmpty(scriptFileRoot)) scriptFileRoot = ".";
            RootDirectory = new DirectoryInfo(scriptFileRoot);

            DBManagerFactory factory = new DBManagerFactory(DBMS.SqlServer2008);

            switch (mode)
            {
                case Mode.Read:
                    IDatabaseReader reader = factory.GetDBReader();
                    reader.ReadFromDB(server.Databases[dbName], RootDirectory);
                    break;
                case Mode.Write:
                    IDatabaseWriter writer = factory.GetDBWriter();
                    writer.WriteToDB(server.Databases[dbName], RootDirectory);
                    break;
                case Mode.Prep:
                    IDatabasePreparer prep = factory.GetDBPreparer();
                    prep.Prep(server.Databases[dbName], RootDirectory);
                    break;
                case Mode.None:
                default: throw new ApplicationException("Mode (e.g. read or write) not specified");
            }
            

            
        }

 
    }

    public enum Mode
    {
        None,
        Read,
        Prep,
        Write
    }
}
