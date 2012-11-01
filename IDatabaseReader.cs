using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using System.IO;

namespace BumbleBee
{
    public interface IDatabaseReader
    {
        void ReadFromDB(Database db, DirectoryInfo dir);
    }
}
