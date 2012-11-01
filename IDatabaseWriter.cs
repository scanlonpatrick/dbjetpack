using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Microsoft.SqlServer.Management.Smo;

namespace DBJetpack
{
    public interface IDatabaseWriter
    {
        void WriteToDB(Database db, DirectoryInfo dir);
    }
}
