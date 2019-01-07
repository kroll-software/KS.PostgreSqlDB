using System.Diagnostics;
using System;
using System.Linq;
using System.Data;
using System.Collections.Generic;
using System.Security;
using System.Security.Cryptography;
using KS.Foundation;
using Npgsql;
using NpgsqlTypes;


namespace KS.PostgreSqlDB
{
    public class PostgreSqlDatabase : DisposableObject
    {
        public NpgsqlConnection DBConn;
        public NpgsqlTransaction Transaction = null;

        public IPasswordEncryptionService PasswordEncryptionService { get; set; }

		public string DBServer { get; set; }
		public string DBPort { get; set; }
		public string DBCatalog { get; set; }
		public string DBUser { get; set; }
		public string DBPassword { get; set; }
		public bool DBWinSecurity { get; set; }    

        //private bool m_IsSysAdmin = false;
		/***
        public bool IsSysAdmin
        {
            get
            {
                return m_IsSysAdmin;
            }
        }
        ***/
        
        public int ConnectionTimeout = 10;
        public int CommandTimeout = 0;


        // *** Constructor ***
        public PostgreSqlDatabase()            
        {
			DBConn = new NpgsqlConnection();
            Reset();
        }			

        private string GetSQLServerConnectionString(string Server, string Port, string Database, string UserID, string Password, long CommandTimeout)
        {            
			string returnValue = "Password=" + Password + ";User ID=" + UserID + ";Database=" + Database + ";Server=" + Server + ";Port=" + Port + ";CommandTimeout=" + CommandTimeout.ToString();
            return returnValue;
        }

        public string ConnectionStringSuffix { get; set; }
        public virtual string ConnectionString
        {
            get
            {
				string retVal = GetSQLServerConnectionString(DBServer, DBPort, DBCatalog, DBUser, DBPassword, ConnectionTimeout);

                if (!ConnectionStringSuffix.IsNullOrEmpty())
                    return retVal.Combine(ConnectionStringSuffix, ';');
                else
                    return retVal;
            }
        }

        public virtual void BeginTransaction()
        {
            if (Transaction != null)
            {
                throw new Exception("Transaction is not NULL");                
            }

            if (DBConn == null)
            {
                throw new Exception("DBConn is NULL");
            }

            this.Transaction = DBConn.BeginTransaction();            
        }

        public virtual void RollbackTransaction()
        {
            if (this.Transaction != null)
            {
                try
                {
                    this.Transaction.Rollback();
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    if (Transaction != null)
                    {
                        //Transaction.Dispose();
                        Transaction = null;
                    }
                }                
            }
        }

        public virtual void CommitTransaction()
        {
            if (this.Transaction != null)
            {
                try
                {
                    this.Transaction.Commit();
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    if (Transaction != null)
                    {
                        //Transaction.Dispose();
                        Transaction = null;
                    }
                }                                
            }
        }        

        public void Reset()
        {
            DBServer = "127.0.0.1";
			DBPort = "5432";
            DBCatalog = "";
            DBWinSecurity = true;
            DBUser = "";
            DBPassword = "";

            ConnectionTimeout = 15;
            CommandTimeout = 40;
        }
        
        public virtual bool Connect()
        {
            try
            {
                if (DBConn == null)
					DBConn = new NpgsqlConnection();

                if (DBConn.State != ConnectionState.Closed)
                    Disconnect();

                DBConn.ConnectionString = ConnectionString;
                DBConn.Open();

                return true;
            }
            catch (Exception ex)
            {
                throw ex;                
            }
        }        

        public virtual void Disconnect()
        {
            // m_CurrentUser = null;
            // m_CurrentUserID = -1;

            if (DBConn == null)
                return;

            if (DBConn.State != ConnectionState.Closed)
            {                
                try
                {
                    DBConn.Close();                    
                }
                catch (Exception)
                {                    
                }                
            }

            //m_IsSysAdmin = false;
        }

        public object ExecuteScalar(string strSQL)
        {
            object retVal = null;

			NpgsqlCommand cmd = null;

            try
            {
				cmd = new NpgsqlCommand(strSQL, DBConn);
                if (Transaction != null)
                    cmd.Transaction = Transaction;

                cmd.CommandTimeout = this.CommandTimeout;
                retVal = cmd.ExecuteScalar();
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    cmd.Dispose();
                }
            }
                        
            return retVal;
        }

        public NpgsqlDataReader ExecuteSQL(string strSQL)
        {
			NpgsqlCommand cmd = null;
			NpgsqlDataReader dr = null;

            try
            {
				cmd = new NpgsqlCommand(strSQL, DBConn);
                if (Transaction != null)
                    cmd.Transaction = Transaction;

                cmd.CommandTimeout = this.CommandTimeout;
                dr = cmd.ExecuteReader();
            }
            catch (Exception)
            {
                if (dr != null)
                {
                    dr.Dispose();
                    dr = null;
                }
                
                throw;
            }
            finally
            {                
                if (cmd != null)
                {
                    cmd.Dispose();
                }
            }
                        
            return dr;
        }

		public NpgsqlDataReader ExecuteStoredProcedure(string storedProcedureName, NpgsqlParameter[] parameters = null)
        {
			NpgsqlCommand cmd = null;
			NpgsqlDataReader dr = null;

            try
            {
				cmd = new NpgsqlCommand(storedProcedureName, DBConn);
                cmd.CommandType = CommandType.StoredProcedure;

                if (parameters != null && parameters.Length > 0)
                    parameters.ForEach(p => cmd.Parameters.Add(p));                

                if (Transaction != null)
                    cmd.Transaction = Transaction;

                cmd.CommandTimeout = this.CommandTimeout;
                dr = cmd.ExecuteReader();
            }
            catch (Exception)
            {
                if (dr != null)
                {
                    dr.Dispose();
                    dr = null;
                }

                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    cmd.Dispose();
                }
            }

            return dr;
        }

        public long ExecuteUpdateSQL(string strSQL)
        {
            long returnValue = 0;

			NpgsqlCommand cmd = null;

            try
            {
				cmd = new NpgsqlCommand(strSQL, DBConn);
                if (Transaction != null)
                    cmd.Transaction = Transaction;

                cmd.CommandTimeout = CommandTimeout;                
                returnValue = cmd.ExecuteNonQuery();
            }
            catch (Exception)
            {                
                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    cmd.Dispose();
                }
            }
                        
            return returnValue;
        }

		public int LastIdentity(string TableName, string ColumnName)
        {
			NpgsqlCommand cmd = null;

            try
            {
				cmd = new NpgsqlCommand(String.Format("SELECT CURRVAL(pg_get_serial_sequence('{0}','{1}'))", TableName, ColumnName), DBConn);
                if (Transaction != null)
                    cmd.Transaction = Transaction;

                return cmd.ExecuteScalar().SafeInt();
            }
            catch (Exception ex)
            {
                throw new Exception("LastIdentity() Error:\r" + ex.Message);
                //modGlobal.ErrMsgBox("LastIdentity() Error:\r" + ex.Message);
            }
            finally
            {
                if (cmd != null)
                    cmd.Dispose();
            }
        }        
			
        public void LoadSettings()
        {
            Reset();

            if (Globals.GetSetting != null)
            {
                DBServer = Globals.GetSetting("DBServer");
                DBCatalog = Globals.GetSetting("DBCatalog");
                DBWinSecurity = Globals.GetSetting("DBIntegratedSecurity").SafeInt(1).SafeBool();
                DBUser = Globals.GetSetting("DBUser");
                if (this.PasswordEncryptionService != null)
                    DBPassword = this.PasswordEncryptionService.DecryptString(Globals.GetSetting("DBPassword").SafeString());

                DBSavePasswords = Globals.GetSetting("DBSavePasswords").SafeBool();
                ConnectionTimeout = Globals.GetSetting("DBConnectionTimeout").SafeInt(15);
                CommandTimeout = Globals.GetSetting("DBCommandTimeout").SafeInt(40);
            }
            else if (!String.IsNullOrEmpty(Globals.RegistrySectionKey))
            {
                KSRegistry cReg = new KSRegistry();
                cReg.sectionKey = Globals.RegistrySectionKey;

                cReg.valueKey = "DBServer";
                cReg.defaultValue = String.Empty;
                DBServer = cReg.value.SafeString();

                cReg.valueKey = "DBCatalog";
                cReg.defaultValue = String.Empty;
                DBCatalog = cReg.value.SafeString();

                cReg.valueKey = "DBIntegratedSecurity";
                cReg.defaultValue = true;
                DBWinSecurity = cReg.value.SafeBool();

                cReg.valueKey = "DBSavePasswords";
                cReg.defaultValue = true;
                DBSavePasswords = cReg.value.SafeBool();

                cReg.valueKey = "DBUser";
                cReg.defaultValue = String.Empty;
                DBUser = cReg.value.SafeString();

                if (this.PasswordEncryptionService != null)
                {
                    cReg.valueKey = "DBPassword";
                    cReg.defaultValue = String.Empty;
                    DBPassword = this.PasswordEncryptionService.DecryptString(cReg.value.SafeString());
                }

                cReg.valueKey = "DBConnectionTimeout";
                cReg.defaultValue = 15;
                ConnectionTimeout = cReg.value.SafeInt(15);

                cReg.valueKey = "DBCommandTimeout";
                cReg.defaultValue = 40;
                CommandTimeout = cReg.value.SafeInt(40);
            }
        }

        public bool DBSavePasswords { get; set; }

        public bool CanSavePasswords
        {
            get
            {
                return DBSavePasswords && this.PasswordEncryptionService != null;
            }
        }

        public void SaveSettings()
        {
            if (Globals.SaveSetting != null)
            {
                Globals.SaveSetting("DBServer", DBServer);
				Globals.SaveSetting("DBPort", DBPort);
                Globals.SaveSetting("DBCatalog", DBCatalog);
                
                Globals.SaveSetting("DBIntegratedSecurity", DBWinSecurity.ToInt().ToString());                

                if (!DBWinSecurity)
                {
                    Globals.SaveSetting("DBSavePasswords", DBSavePasswords.ToInt().ToString());
                    Globals.SaveSetting("DBUser", DBUser);
                    if (CanSavePasswords)
                        Globals.SaveSetting("DBPassword", this.PasswordEncryptionService.EncryptString(DBPassword));
                }
                else
                {
                    Globals.SaveSetting("DBSavePasswords", String.Empty);
                    Globals.SaveSetting("DBUser", String.Empty);
                    Globals.SaveSetting("DBPassword", String.Empty);                    
                }

                Globals.SaveSetting("DBConnectionTimeout", ConnectionTimeout.ToString());
                Globals.SaveSetting("DBCommandTimeout", CommandTimeout.ToString());
            }
            else if (!String.IsNullOrEmpty(Globals.RegistrySectionKey))
            {
                KSRegistry cReg = new KSRegistry();
                cReg.sectionKey = Globals.RegistrySectionKey;

                cReg.valueKey = "DBServer";
                cReg.defaultValue = String.Empty;
                cReg.value = DBServer;

				cReg.valueKey = "DBPort";
				cReg.defaultValue = String.Empty;
				cReg.value = DBPort;

                cReg.valueKey = "DBCatalog";
                cReg.defaultValue = String.Empty;
                cReg.value = DBCatalog;

                cReg.valueKey = "DBIntegratedSecurity";
                cReg.defaultValue = false;
                cReg.value = DBWinSecurity;                

                if (!DBWinSecurity)
                {
                    cReg.valueKey = "DBSavePasswords";
                    cReg.defaultValue = false;
                    cReg.value = DBSavePasswords.ToInt();

                    cReg.valueKey = "DBUser";
                    cReg.defaultValue = String.Empty;
                    cReg.value = DBUser;

                    if (CanSavePasswords)
                    {
                        cReg.valueKey = "DBPassword";
                        cReg.defaultValue = String.Empty;
                        cReg.value = this.PasswordEncryptionService.EncryptString(DBPassword);
                    }
                }
                else
                {
                    cReg.valueKey = "DBSavePasswords";
                    cReg.defaultValue = String.Empty;
                    cReg.value = String.Empty;

                    cReg.valueKey = "DBUser";
                    cReg.defaultValue = String.Empty;
                    cReg.value = String.Empty;

                    cReg.valueKey = "DBPassword";
                    cReg.defaultValue = String.Empty;
                    cReg.value = String.Empty;
                }

                cReg.valueKey = "DBConnectionTimeout";
                cReg.defaultValue = 20;
                cReg.value = ConnectionTimeout;

                cReg.valueKey = "DBCommandTimeout";
                cReg.defaultValue = 40;
                cReg.value = CommandTimeout;
            }
        }

        private bool UseDefaultDB()
        {
            try
            {
                this.ExecuteUpdateSQL("USE " + DBCatalog);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        
        public bool IsConnected
        {
            get
            {
                return (DBConn != null && DBConn.State != 0);
            }            
        }


        protected override void CleanupManagedResources()
        {
            lock (SyncObject)
            {
                if (Transaction != null)
                {
                    Transaction.Dispose();
                    //Transaction = null;
                }

                if (DBConn != null)
                {
                    if (IsConnected)
                        Disconnect(); // this throws errors

                    DBConn.Dispose();
                    DBConn = null;
                }
            }

            base.CleanupManagedResources();
        }
    }

    public static class SqlDatabaseExtensions
    {
		/***
        public static void SetUpPasswordEncryption(this SqlDatabase db, string passwordEncryptionEntropySource)
        {
            db.DBSavePasswords = true;
            db.PasswordEncryptionService = new PasswordEncryptionService(passwordEncryptionEntropySource);
        }
			
        public static void SetUpPasswordEncryption(this Dialogs.CreateNewDatabaseDialog dialog, string passwordEncryptionEntropySource)
        {
            dialog.SavePasswords = true;
            dialog.PasswordEncryptionService = new PasswordEncryptionService(passwordEncryptionEntropySource);
        }
        ***/
    }
}
