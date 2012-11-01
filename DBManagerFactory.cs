using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BumbleBee
{
    class DBManagerFactory
    {
        DBMS _dbms;
        public DBManagerFactory(DBMS dbms)
        {
            this._dbms = dbms;
        }

        public IDatabaseReader GetDBReader()
        {
            switch (this._dbms)
            {
                case DBMS.SqlServer2008:
                    return new DBManagers.SqlServer2008Reader();
                case DBMS.SqlServer2005:
                    throw new NotImplementedException();
                default:
                    throw new NotImplementedException();
            }
        }

        public IDatabaseWriter GetDBWriter()
        {
            switch (this._dbms)
            {
                case DBMS.SqlServer2008:
                    return new DBManagers.SqlServer2008Writer();
                case DBMS.SqlServer2005:
                    throw new NotImplementedException();
                default:
                    throw new NotImplementedException();
            }
        }

        public IDatabasePreparer GetDBPreparer()
        {
            switch (this._dbms)
            {
                case DBMS.SqlServer2008:
                    return new DBManagers.SqlServer2008Preparer();
                case DBMS.SqlServer2005:
                    throw new NotImplementedException();
                default:
                    throw new NotImplementedException();
            }
        }
    }
}
