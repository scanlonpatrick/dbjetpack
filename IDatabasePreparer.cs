using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.Smo;
using System.IO;

namespace BumbleBee
{
    public interface IDatabasePreparer
    {
        void Prep(Database db, DirectoryInfo dir);
    }
}
