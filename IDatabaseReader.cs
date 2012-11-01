using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using System.IO;

namespace DBJetpack
{
    public interface IDatabaseReader
    {
        void ReadFromDB(Database db, DirectoryInfo dir);
    }
}
