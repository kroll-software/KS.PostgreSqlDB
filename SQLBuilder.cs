using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Npgsql;
using NpgsqlTypes;
using System.Text;
using KS.Foundation;

namespace KS.PostgreSqlDB
{
    public enum SQLBuilderTypes
    {
        sqlString = 0,
        sqlBool = 1,
        sqlInt = 2,
        sqlFloat = 3,
        sqlDate = 4,
        sqlMemo = 5,
        sqlChar = 6,
        sqlMoney = 7,
        sqlTime = 8
    }
			
    public enum SQLServerBoolTypes
    {
        boolBit = 0,
        boolTinyInt = 1
    }

    public class SQLColumnDefinition
    {
        public SQLBuilderTypes SQLType;
        public bool AllowNull;
        public int MaxLen;
        public object Value;
        public object DefaultValue;
        public bool bActive = true;
        public bool bWhere = false;

        public SQLColumnDefinition()
        {
            Reset();
        }

        public void Reset()
        {
            SQLType = SQLBuilderTypes.sqlString;
            AllowNull = true;
            MaxLen = 255;
            Value = null;
            bActive = true;
            bWhere = false;
        }

        public string DBValueString(System.Globalization.NumberFormatInfo nfi)
        {
            string strValue;

            if ((Value == null || Value.Equals(DBNull.Value)) && this.AllowNull)
                return "NULL";

            switch (SQLType)
            {
                case SQLBuilderTypes.sqlString:
                case SQLBuilderTypes.sqlChar:
                    strValue = Value.SafeString();
                    if (MaxLen > 0)
                        strValue = Strings.StrLeft(strValue, MaxLen);

                    if (AllowNull)
                        strValue = Strings.ConvertNullSQL(strValue);
                    else
                        strValue = "\'" + Strings.DoubleQuotes(strValue) + "\'";

                    break;

                case SQLBuilderTypes.sqlInt:
                    if (Value.IsNumeric())
                        strValue = Value.SafeInt().ToString(nfi);
                    else
                    {
                        if (AllowNull)
                            strValue = "NULL";
                        else
                            strValue = DefaultValue.SafeInt().ToString(nfi);
                    }

                    break;

                case SQLBuilderTypes.sqlFloat:                
                    if (Value.IsNumeric())
                    {
                        strValue = Value.SafeDouble().ToString(nfi);
                    }
                    else
                    {
                        if (AllowNull)
                            strValue = "NULL";
                        else
                            strValue = DefaultValue.SafeDouble().ToString(nfi);
                    }

                    break;

                case SQLBuilderTypes.sqlMoney:
                    if (Value.IsNumeric())
                    {
                        strValue = Value.SafeDecimal().ToString(nfi);
                    }
                    else
                    {
                        if (AllowNull)
                            strValue = "NULL";
                        else
                            strValue = DefaultValue.SafeDecimal().ToString(nfi);
                    }

                    break;

                case SQLBuilderTypes.sqlBool:
					strValue = Value.SafeBool().ToInt().ToString();
                    //strValue = Value.SafeBool().ToString(nfi).ToUpper();
                    break;


                case SQLBuilderTypes.sqlDate:
                    if (Value.IsDate())
                    {
                        if (AllowNull && (DateTime)Value == DateTime.MinValue)
                            strValue = "NULL";
                        else
                            strValue = Strings.SQLDateTime(System.Convert.ToDateTime(Value), true);
                    }
                    else
                    {
                        if (AllowNull || !DefaultValue.IsDate())
                            strValue = "NULL";
                        else
						strValue = Strings.SQLDateTime(System.Convert.ToDateTime(DefaultValue), true);
                    }
                    break;

                case SQLBuilderTypes.sqlMemo:
                    strValue = Value.SafeString();
                    if (AllowNull)
                        strValue = Strings.ConvertNullSQL(strValue);
                    else
                        strValue = "\'" + Strings.DoubleQuotes(strValue) + "\'";

                    break;

                case SQLBuilderTypes.sqlTime:
                    if (Value.IsTimeSpan())
                    {
                        if (AllowNull && Value.SafeTimeSpan() == TimeSpan.Zero)
                            strValue = "NULL";
                        else
                            strValue = Value.ToString();
                    }
                    else
                    {
                        if (AllowNull || !DefaultValue.IsTimeSpan())
                            strValue = "NULL";
                        else
                            strValue = DefaultValue.ToString();
                    }
                    break;

                default:
                    strValue = "NULL";
                    break;
            }

            return strValue;
        }

        public object DBValue()
        {
            if ((Value == null || Value.Equals(DBNull.Value)) && this.AllowNull)
                return DBNull.Value;

            switch (SQLType)
            {
                case SQLBuilderTypes.sqlString:
                case SQLBuilderTypes.sqlChar:
                    string strValue = Value.SafeString();

                    if (strValue.Length == 0 && this.AllowNull)
                        return DBNull.Value;

                    if (MaxLen > 0)
                        strValue = Strings.StrLeft(strValue, MaxLen);

                    return strValue;

                case SQLBuilderTypes.sqlInt:
                    if (Value.IsNumeric())
                        return Value.SafeInt();
                    else
                    {
                        if (AllowNull)
                            return DBNull.Value;
                        else
                            return DefaultValue.SafeInt();
                    }

                case SQLBuilderTypes.sqlFloat:                
                    if (Value.IsNumeric())
                        return Value.SafeDouble();
                    else
                    {
                        if (AllowNull)
                            return DBNull.Value;
                        else
                            return DefaultValue.SafeDouble();
                    }

                case SQLBuilderTypes.sqlMoney:
                    if (Value.IsNumeric())
                        return Value.SafeDecimal();
                    else
                    {
                        if (AllowNull)
                            return DBNull.Value;
                        else
                            return DefaultValue.SafeDecimal();
                    }

                case SQLBuilderTypes.sqlBool:
                    return Value.SafeBool().ToInt();
                    // return Value.SafeBool();


                case SQLBuilderTypes.sqlDate:
                    if (Value.IsDate())
                    {
                        if (AllowNull && Value.SafeDateTime() == DateTime.MinValue)
                            return DBNull.Value;
                        else
                            return Value;
                    }
                    else
                    {
                        if (AllowNull || !DefaultValue.IsDate())
                            return DBNull.Value;
                        else
                            //return Value.SafeDateTime();
                            return DefaultValue.SafeDateTime();
                    }

                case SQLBuilderTypes.sqlMemo:
                    string strValueMemo = Value.SafeString();

                    if (strValueMemo.Length == 0 && this.AllowNull)
                        return DBNull.Value;

                    return strValueMemo;

                case SQLBuilderTypes.sqlTime:
                    if (Value.IsTimeSpan())
                    {
                        if (AllowNull && Value.SafeTimeSpan() == TimeSpan.Zero)
                            return DBNull.Value;
                        else
                            return Value;
                    }
                    else
                    {
                        if (AllowNull || !DefaultValue.IsTimeSpan())
                            return DBNull.Value;
                        else
                            return DefaultValue.SafeTimeSpan();
                    }

                default:
                    return DBNull.Value;
            }

        }
    }


    public class SQLBuilder : DisposableObject
    {
        private string pTablename;

        private Dictionary<string, SQLColumnDefinition> Columns;
        private List<string> ColumnNames;        
        public SQLServerBoolTypes SQLServerBoolType = SQLServerBoolTypes.boolTinyInt;

        private System.Globalization.NumberFormatInfo nfi;

        public NpgsqlConnection SQLConn;        
		private NpgsqlCommand cmdSQLInsert = null;
		private NpgsqlCommand cmdSQLUpdate = null;
		private NpgsqlCommand cmdSQLDelete = null;
        
        private NpgsqlTransaction m_SQLTrans = null;
		public NpgsqlTransaction SQLTrans
        {
            get
            {
                return m_SQLTrans;
            }
            set
            {
                if (m_SQLTrans != value)
                {
                    m_SQLTrans = value;
                    
                    if (cmdSQLInsert != null)                    
                        cmdSQLInsert.Transaction = value;
                    
                    if (cmdSQLUpdate != null)                    
                        cmdSQLUpdate.Transaction = value;
                    
                    if (cmdSQLDelete != null)
                        cmdSQLDelete.Transaction = value;                    
                }
            }
        }

        public SQLBuilder(string Tablename, IDbConnection conn, IDbTransaction trans)
        {
            pTablename = Tablename;
            Columns = new Dictionary<string, SQLColumnDefinition>();
            ColumnNames = new List<string>();

            if (conn != null)
            {				
				SQLConn = (NpgsqlConnection)conn;
            }

            if (trans != null)
            {		
				SQLTrans = (NpgsqlTransaction)trans;
            }

            nfi = new System.Globalization.CultureInfo("en-US", false).NumberFormat;
            nfi.NumberGroupSeparator = "";
        }

        public void ResetColumns(string Tablename)
        {
            Columns.Clear();
            ColumnNames.Clear();

            if (Tablename != "")
            {
                pTablename = Tablename;
            }

            ResetParameters();
        }

        public void ResetData()
        {
            ResetData(true);
        }

        public void ResetData(bool resetParameters)
        {
            foreach (SQLColumnDefinition d in Columns.Values)
            {
                d.Value = null;                
            }

            if (resetParameters)
                ResetParameters();
        }

        private void ResetParameters()
        {
            if (cmdSQLInsert != null)
            {                
                cmdSQLInsert.Dispose();
                cmdSQLInsert = null;
            }

            if (cmdSQLUpdate != null)
            {
                cmdSQLUpdate.Dispose();
                cmdSQLUpdate = null;
            }
				            
            if (cmdSQLDelete != null)
            {
                cmdSQLDelete.Dispose();
                cmdSQLDelete = null;
            }
        }

        public int GetActiveCount()
        {
            int i = 0;
            foreach (SQLColumnDefinition d in Columns.Values)
            {
                if (d.bActive)
                    i++;
            }

            return i;
        }        

        public void AddColumn(string ColumnName)
        {
            AddColumn(ColumnName, SQLBuilderTypes.sqlString, true, 255, "");
        }

        public void AddColumn(string ColumnName, SQLBuilderTypes SQLType)
        {
            AddColumn(ColumnName, SQLType, true, 255, "");
        }

        public void AddColumn(string ColumnName, SQLBuilderTypes SQLType, bool AllowNull)
        {
            AddColumn(ColumnName, SQLType, AllowNull, 255, "");
        }

        public void AddColumn(string ColumnName, SQLBuilderTypes SQLType, bool AllowNull, int MaxLen)
        {
            AddColumn(ColumnName, SQLType, AllowNull, MaxLen, "");
        }

        public void AddColumn(string ColumnName, SQLBuilderTypes SQLType, bool AllowNull, int MaxLen, object DefaultValue)
        {
            SQLColumnDefinition d = new SQLColumnDefinition();
            d.SQLType = SQLType;
            d.AllowNull = AllowNull;
            d.MaxLen = MaxLen;
            d.Value = null;

            if (DefaultValue == null)
            {
                switch (SQLType)
                {
                    case SQLBuilderTypes.sqlString:
                    case SQLBuilderTypes.sqlMemo:
                    case SQLBuilderTypes.sqlChar:
                        d.DefaultValue = "";
                        break;

                    case SQLBuilderTypes.sqlBool:
                        d.DefaultValue = false;
                        break;

                    case SQLBuilderTypes.sqlInt:
                        d.DefaultValue = 0;
                        break;

                    case SQLBuilderTypes.sqlFloat:
                        d.DefaultValue = 0;
                        break;

                    case SQLBuilderTypes.sqlDate:
                        d.DefaultValue = new DateTime(1990, 1, 1);
                        break;
                }
            }
            else
            {
                d.DefaultValue = DefaultValue;
            }


            if (!Columns.ContainsKey(ColumnName))
            {
                Columns.Add(ColumnName, d);
                ColumnNames.Add(ColumnName);
            }
        }

        public bool AddValue(string ColumnName, object Data)
        {
            if (!Columns.ContainsKey(ColumnName))
            {
                System.Diagnostics.Debug.Assert(false, "ColumnName not found.");
                return false;
            }

            SQLColumnDefinition d = Columns[ColumnName];
            if (d == null)
                return false;            

            d.Value = Data;
            return true;
        }

        public void ResetColumnsActive()
        {
            foreach (SQLColumnDefinition d in Columns.Values)
            {
                d.bActive = true;
                d.bWhere = false;
            }
        }

        public void SetColumnActive(string ColumnName, bool bActive)
        {
            if (Columns.ContainsKey(ColumnName))
                Columns[ColumnName].bActive = bActive;
        }

        public void SetColumnWhere(string ColumnName, bool bWhere)
        {
            if (Columns.ContainsKey(ColumnName))
                Columns[ColumnName].bWhere = bWhere;
        }

        public bool GetColumnActive(string ColumnName)
        {
            if (Columns.ContainsKey(ColumnName))
                return Columns[ColumnName].bActive;
            else
                return false;
        }

        public bool GetColumnWhere(string ColumnName)
        {
            if (Columns.ContainsKey(ColumnName))
                return Columns[ColumnName].bWhere;
            else
                return false;
        }

        public string InsertCommand()
        {
            System.Text.StringBuilder SB = new System.Text.StringBuilder();

            SB.Append("insert into ");
            SB.Append(pTablename);
            SB.Append(" (");

            SQLColumnDefinition d;

            bool bFlag = false;
            foreach (string f in ColumnNames)
            {
                d = Columns[f];
                if (d.bActive)
                {
                    if (bFlag)
                        SB.Append(", ");

                    SB.Append(f);
                    bFlag = true;
                }
            }

            SB.Append(") VALUES (");

            bFlag = false;
            foreach (string f in ColumnNames)
            {
                d = Columns[f];
                if (d.bActive)
                {
                    if (bFlag)
                        SB.Append(", ");

                    SB.Append(d.DBValueString(nfi));
                    bFlag = true;
                }
            }

            SB.Append(")");

            return SB.ToString();
        }

        public string UpdateCommand(string WhereClause)
        {
            System.Text.StringBuilder SB = new System.Text.StringBuilder();

            SB.Append("UPDATE ");
            SB.Append(pTablename);
            SB.Append(" SET ");

            SQLColumnDefinition d;

            bool bFlag = false;
            foreach (string f in ColumnNames)
            {
                d = Columns[f];
                if (d.bActive)
                {

                    if (bFlag)
                        SB.Append(", ");

                    SB.Append(f);
                    SB.Append("=");

                    SB.Append(d.DBValueString(nfi));
                    bFlag = true;
                }
            }

            SB.Append(" WHERE (");
            SB.Append(WhereClause);
            SB.Append(")");

            return SB.ToString();
        }

        public string DeleteCommand(string WhereClause)
        {
            System.Text.StringBuilder SB = new System.Text.StringBuilder();

            SB.Append("DELETE FROM ");
            SB.Append(pTablename);
            SB.Append(" WHERE (");
            SB.Append(WhereClause);
            SB.Append(")");

            return SB.ToString();
        }

        private void SetCommandParameters(ref NpgsqlCommand cmd)
        {
            SQLColumnDefinition def;

            foreach (string ColumnName in ColumnNames)
            {
                def = Columns[ColumnName];

                if (def.bActive || def.bWhere)
                {
                    switch (def.SQLType)
                    {
                        case SQLBuilderTypes.sqlString:
                            cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), NpgsqlDbType.Varchar, def.MaxLen);
                            break;

                        case SQLBuilderTypes.sqlInt:
						cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), NpgsqlDbType.Integer);
                            break;

                        case SQLBuilderTypes.sqlDate:
						cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), NpgsqlDbType.Date);
                            break;

                        case SQLBuilderTypes.sqlFloat:
						cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), NpgsqlDbType.Real);
                            break;

                        case SQLBuilderTypes.sqlMemo:
						cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), NpgsqlDbType.Text);
                            break;

                        case SQLBuilderTypes.sqlBool:
                            if (SQLServerBoolType == SQLServerBoolTypes.boolTinyInt)
							cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), NpgsqlDbType.Smallint);
                            else
							cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), NpgsqlDbType.Bit);
                            break;

                        case SQLBuilderTypes.sqlChar:
						cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), NpgsqlDbType.Char, def.MaxLen);
                            break;

                        case SQLBuilderTypes.sqlMoney:
						cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), NpgsqlDbType.Money);
                            break;

                        case SQLBuilderTypes.sqlTime:
						cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), NpgsqlDbType.Time);
                            break;
                    }
                }
            }
        }


        private void SetCommandParameters(ref System.Data.OleDb.OleDbCommand cmd)
        {
            SQLColumnDefinition def;

            foreach (string ColumnName in ColumnNames)
            {
                def = Columns[ColumnName];

                if (def.bActive || def.bWhere)
                {
                    switch (def.SQLType)
                    {
                        case SQLBuilderTypes.sqlString:
                            cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), System.Data.OleDb.OleDbType.VarWChar, def.MaxLen);
                            break;

                        case SQLBuilderTypes.sqlInt:
                            cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), System.Data.OleDb.OleDbType.Integer);
                            break;

                        case SQLBuilderTypes.sqlDate:
                            cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), System.Data.OleDb.OleDbType.Date);
                            break;

                        case SQLBuilderTypes.sqlFloat:
                            cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), System.Data.OleDb.OleDbType.Double);
                            break;

                        case SQLBuilderTypes.sqlMemo:
                            cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), System.Data.OleDb.OleDbType.LongVarWChar);
                            break;

                        case SQLBuilderTypes.sqlBool:
                            cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), System.Data.OleDb.OleDbType.Boolean);
                            break;

                        case SQLBuilderTypes.sqlChar:
                            cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), System.Data.OleDb.OleDbType.WChar, def.MaxLen);
                            break;

                        case SQLBuilderTypes.sqlMoney:
                            cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), System.Data.OleDb.OleDbType.Double);
                            break;

                        case SQLBuilderTypes.sqlTime:
                            cmd.Parameters.Add("@" + Column2ParameterName(ColumnName), System.Data.OleDb.OleDbType.DBTime);
                            break;
                    }
                }
            }
        }


        private string Column2ParameterName(string S)
        {
            if (S.Length > 1 && S[0] == '[' && S[S.Length - 1] == ']')
                return S.Substring(1, S.Length - 2);
            else
                return S;
        }

        // <<<<<<<<<<<<<<<<<<<<<<< DataSet >>>>>>>>>>>>>>>>>>>>>>>>>

		/***
        public bool InsertIntoDataSet(DataSet DS)
        {
            //int index = DS.Tables.IndexOf(null, pTablename);
            //DataTable dt = DS.Tables[index];
            DataRow row = DS.Tables[pTablename].NewRow();

            foreach (string ColumnName in ColumnNames)
            {
                SQLColumnDefinition d = Columns[ColumnName];
                if (d.bActive)
                {                    
                    row[ColumnName] = d.DBValue(pDBMSType);
                }
            }

            DS.Tables[pTablename].Rows.Add(row);
            return true;
        }
        ***/

        private string ActiveColumnsList()
        {
            return String.Join(", ", ColumnNames.Where(p => Columns[p].bActive));
        }

        private string ActiveParameterNameList()
        {
            return String.Join(", ", ColumnNames.Where(p => Columns[p].bActive).Select(p => "@" + Column2ParameterName(p)));
        }

        private string ActiveColumnEqualParameterList()
        {
            return String.Join(" AND ", ColumnNames.Where(p => Columns[p].bActive).Select(p => p + "=@" + Column2ParameterName(p)));
        }

        private string ActiveColumnSetParameterList()
        {
            return String.Join(", ", ColumnNames.Where(p => Columns[p].bActive).Select(p => p + "=@" + Column2ParameterName(p)));
        }

        private string WhereColumnEqualParameterList()
        {
            return String.Join(" AND ", ColumnNames.Where(p => Columns[p].bWhere).Select(p => p + "=@" + Column2ParameterName(p)));
        }

		public int Insert()
		{
			return SqlInsertCommandObject (false, false).ExecuteNonQuery ();
		}

        // <<<<<<<<<<<<<<<<<<<<<<< SQL Server >>>>>>>>>>>>>>>>>>>>>>>>>        
		public NpgsqlCommand SqlInsertCommandObject(bool Prepared = false, bool WithIfNotExist = false)
        {            
			if (cmdSQLInsert == null && WithIfNotExist)
            {
				cmdSQLInsert = new NpgsqlCommand();
                cmdSQLInsert.Connection = SQLConn;
                if (m_SQLTrans != null)
                    cmdSQLInsert.Transaction = m_SQLTrans;

                StringBuilder SB = new StringBuilder();

                // --- if not exist --
                SB.Append("IF NOT EXISTS (SELECT ");
                SB.Append(ActiveColumnsList());
                SB.Append(" FROM ");
                SB.Append(pTablename);
                SB.Append(" WHERE ");
                SB.Append(ActiveColumnEqualParameterList());
                SB.Append(") ");                
                // --- if not exist --

                SB.Append("Insert into " + pTablename + " (");
                SB.Append(ActiveColumnsList());
                SB.Append(") Values (");
                SB.Append(ActiveParameterNameList());
                SB.Append(")");

                cmdSQLInsert.CommandText = SB.ToString();

                SetCommandParameters(ref cmdSQLInsert);

                if (Prepared)
                    cmdSQLInsert.Prepare();
            }

            return SqlInsertCommandObject(Prepared);
        }

		public NpgsqlCommand SqlInsertCommandObject(bool Prepared = false)
        {
            SQLColumnDefinition def;

            if (cmdSQLInsert == null)
            {
				cmdSQLInsert = new NpgsqlCommand();
                cmdSQLInsert.Connection = SQLConn;
                if (m_SQLTrans != null)
                    cmdSQLInsert.Transaction = m_SQLTrans;

                StringBuilder SB = new StringBuilder();

                SB.Append("Insert into " + pTablename + " (");
                SB.Append(ActiveColumnsList());
                SB.Append(") Values (");
                SB.Append(ActiveParameterNameList());
                SB.Append(")");

                cmdSQLInsert.CommandText = SB.ToString();

                SetCommandParameters(ref cmdSQLInsert);

                if (Prepared)
                    cmdSQLInsert.Prepare();
            }

            // Data
            foreach (string ColumnName in ColumnNames)
            {
                def = Columns[ColumnName];
                if (def.bActive)
                {
                    cmdSQLInsert.Parameters["@" + Column2ParameterName(ColumnName)].Value = def.DBValue();
                }
            }

            return cmdSQLInsert;
        }


		public NpgsqlCommand SqlUpdateCommandObject()
        {
            SQLColumnDefinition def;

            if (cmdSQLUpdate == null)
            {
				cmdSQLUpdate = new NpgsqlCommand();
                cmdSQLUpdate.Connection = SQLConn;
                if (m_SQLTrans != null)
                    cmdSQLUpdate.Transaction = m_SQLTrans;

                StringBuilder SB = new StringBuilder();
                SB.Append("Update " + pTablename + " set ");
                SB.Append(ActiveColumnSetParameterList());

                SB.Append(" WHERE ");
                SB.Append(WhereColumnEqualParameterList());

                cmdSQLUpdate.CommandText = SB.ToString();

                SetCommandParameters(ref cmdSQLUpdate);
            }

            // Data
            foreach (string ColumnName in ColumnNames)
            {
                def = Columns[ColumnName];
                if (def.bActive)
                {
                    cmdSQLUpdate.Parameters["@" + Column2ParameterName(ColumnName)].Value = def.DBValue();
                }
            }

            return cmdSQLUpdate;
        }

		public NpgsqlCommand SqlUpdateCommandObject(string strWhere)
        {
            SQLColumnDefinition def;

            if (cmdSQLUpdate == null)
            {
				cmdSQLUpdate = new NpgsqlCommand();
                cmdSQLUpdate.Connection = SQLConn;
                if (m_SQLTrans != null)
                    cmdSQLUpdate.Transaction = m_SQLTrans;

                StringBuilder SB = new StringBuilder();
                SB.Append("Update " + pTablename + " set ");
                SB.Append(ActiveColumnSetParameterList());

                SB.Append(" WHERE ");
                SB.Append(strWhere);

                cmdSQLUpdate.CommandText = SB.ToString();

                SetCommandParameters(ref cmdSQLUpdate);
            }
            else
            {
                // Nur WHERE ändern !
                int i = Strings.InStr(cmdSQLUpdate.CommandText, " WHERE ");
                cmdSQLUpdate.CommandText = Strings.StrLeft(cmdSQLUpdate.CommandText, i - 1) + " WHERE " + strWhere;
                cmdSQLUpdate.Transaction = m_SQLTrans;
            }

            // Data
            foreach (string ColumnName in ColumnNames)
            {
                def = Columns[ColumnName];
                if (def.bActive)
                {
                    cmdSQLUpdate.Parameters["@" + Column2ParameterName(ColumnName)].Value = def.DBValue();
                }
            }

            return cmdSQLUpdate;
        }

		public NpgsqlCommand SqlDeleteCommandObject(bool Prepared)
        {
            SQLColumnDefinition def;

            if (cmdSQLDelete == null)
            {
				cmdSQLDelete = new NpgsqlCommand();
                cmdSQLDelete.Connection = SQLConn;
                if (m_SQLTrans != null)
                    cmdSQLDelete.Transaction = m_SQLTrans;

                string strSQL = "Delete from " + pTablename + " WHERE " + WhereColumnEqualParameterList();

                cmdSQLDelete.CommandText = strSQL;

                SetCommandParameters(ref cmdSQLDelete);

                if (Prepared)
                    cmdSQLDelete.Prepare();
            }

            // Data
            foreach (string ColumnName in ColumnNames)
            {
                def = Columns[ColumnName];
                if (def.bActive)
                {
                    cmdSQLDelete.Parameters["@" + Column2ParameterName(ColumnName)].Value = def.DBValue();
                }
            }

            return cmdSQLDelete;
        }

		public NpgsqlCommand SqlDeleteCommandObject(string strWhere, bool Prepared)
        {
            if (cmdSQLDelete == null)
            {
				cmdSQLDelete = new NpgsqlCommand();
                cmdSQLDelete.Connection = SQLConn;
                if (m_SQLTrans != null)
                    cmdSQLDelete.Transaction = m_SQLTrans;

                string strSQL = "Delete from " + pTablename + " WHERE " + strWhere;

                cmdSQLDelete.CommandText = strSQL;

                if (Prepared)
                    cmdSQLDelete.Prepare();
            }
            else
            {
                // Nur WHERE ändern !
                int i = Strings.InStr(cmdSQLDelete.CommandText, " WHERE ");
                cmdSQLDelete.CommandText = Strings.StrLeft(cmdSQLDelete.CommandText, i - 1) + " WHERE " + strWhere;
                cmdSQLDelete.Transaction = m_SQLTrans;
            }

            return cmdSQLDelete;
        }

        // ********* IDisposable **********

        protected override void CleanupManagedResources()
        {
            // CleanUp here and set to null
            if (cmdSQLInsert != null)
            {
                cmdSQLInsert.Dispose();
                cmdSQLInsert = null;
            }

            if (cmdSQLUpdate != null)
            {
                cmdSQLUpdate.Dispose();
                cmdSQLUpdate = null;
            }

            if (cmdSQLDelete != null)
            {
                cmdSQLDelete.Dispose();
                cmdSQLDelete = null;
            }
        }
    }

}
