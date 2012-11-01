using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.Smo;
using System.IO;

namespace DBJetpack
{
    public interface IDatabasePreparer
    {
        void Prep(Database db, DirectoryInfo dir);
    }
}
