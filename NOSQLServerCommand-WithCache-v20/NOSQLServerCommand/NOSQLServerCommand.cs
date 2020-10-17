using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Net.Sockets;
using System.IO;

namespace NOSQLServerCommandNS
{
    public class NOSQLServerCommand
    {
        public String db, password, server_ip;
        public int port;
        private String request_xml = "";

        public List<String> colnames;
        public List<String> types;
        public List<List<String>> records;
        public String message;
        public Int64 last_insert_id_or_affected_rows;
        public Boolean hasrows;

        private static byte[] inStream = new byte[1000000];

        public NOSQLServerCommand(String db, String password, String server_ip, int port = 9999)
        {
            this.db = db;
            this.password = password;
            this.server_ip = server_ip;
            this.port = port;
        }

        private string XmlEscape(string unescaped)
        {
            XmlDocument doc = new XmlDocument();
            XmlNode node = doc.CreateElement("root");
            node.InnerText = unescaped;
            return node.InnerXml;
        }

        private string XmlUnescape(string escaped)
        {
            XmlDocument doc = new XmlDocument();
            XmlNode node = doc.CreateElement("root");
            node.InnerXml = escaped;
            return node.InnerText;
        }

        public NOSQLServerCommand select(String table_name, String[] columns)
        {
            request_xml += "<command>select</command>\n";
            request_xml += "<select>\n";
            request_xml += "<table_name>" + XmlEscape(table_name) + "</table_name>\n";
            for (int i = 0; i < columns.Length; ++i)
                request_xml += "<col>" + XmlEscape(columns[i]) + "</col>\n";
            request_xml += "</select>\n";
            return this;
        }

        public NOSQLServerCommand join(String table_name, String on, String type = "inner")
        {
            request_xml += "<join>\n";
            request_xml += "<table_name>" + XmlEscape(table_name) + "</table_name>\n";
            request_xml += "<on>" + XmlEscape(on) + "</on>\n";
            request_xml += "<type>" + XmlEscape(type) + "</type>\n";
            request_xml += "</join>\n";
            return this;
        }

        public NOSQLServerCommand where(String expression, Boolean with_index = false)
        {
            request_xml += "<where>\n";
            request_xml += "<expression>" + XmlEscape(expression) + "</expression>\n";
            request_xml += "<withindex>" + XmlEscape(with_index.ToString()) + "</withindex>\n";
            request_xml += "</where>\n";
            return this;
        }

        public NOSQLServerCommand having(String expression)
        {
            request_xml += "<having>\n";
            request_xml += "<expression>" + XmlEscape(expression) + "</expression>\n";
            request_xml += "</having>\n";
            return this;
        }

        public NOSQLServerCommand orderby(String[] columns, String[] types = null)
        {
            for (int i = 0; i < columns.Length; ++i)
                request_xml += "<orderby>" + XmlEscape(columns[i]) + "</orderby>\n";

            if (types != null)
                for (int i = 0; i < types.Length; ++i)
                    request_xml += "<orderbytype>" + XmlEscape(types[i]) + "</orderbytype>\n";

            return this;
        }

        public NOSQLServerCommand groupby(String columns)
        {
            request_xml += "<groupby>" + XmlEscape(columns) + "</groupby>\n";
            return this;
        }

        public NOSQLServerCommand limit(int start_index, int record_count)
        {
            request_xml += "<limit>" + start_index + "," + record_count + "</limit>\n";
            return this;
        }

        public NOSQLServerCommand createtable(String table_name, String[] columns)
        {
            request_xml += "<command>createtable</command>\n";
            request_xml += "<createtable>\n";
            request_xml += "<table_name>" + XmlEscape(table_name) + "</table_name>\n";
            for (int i = 0; i < columns.Length; ++i)
                request_xml += "<col>" + XmlEscape(columns[i]) + "</col>\n";
            request_xml += "</createtable>\n";
            return this;
        }

        public NOSQLServerCommand createindex(String index_name, String table_name, Boolean unique, String[] columns)
        {
            request_xml += "<command>createindex</command>\n";
            request_xml += "<createindex>\n";
            request_xml += "<name>" + XmlEscape(index_name) + "</name>\n";
            request_xml += "<table_name>" + XmlEscape(table_name) + "</table_name>\n";
            request_xml += "<unique>" + unique + "</unique>\n";

            for (int i = 0; i < columns.Length; ++i)
                request_xml += "<col>" + XmlEscape(columns[i]) + "</col>\n";
            request_xml += "</createindex>\n";
            return this;
        }

        public NOSQLServerCommand insertinto(String table_name, String[] columns)
        {
            request_xml += "<command>insertinto</command>\n";
            request_xml += "<insertinto>\n";
            request_xml += "<table_name>" + XmlEscape(table_name) + "</table_name>\n";
            for (int i = 0; i < columns.Length; ++i)
                request_xml += "<col>" + XmlEscape(columns[i]) + "</col>\n";
            request_xml += "</insertinto>\n";
            return this;
        }

        public NOSQLServerCommand values(String[] values)
        {
            request_xml += "<values>\n";
            for (int i = 0; i < values.Length; ++i)
                request_xml += "<val>" + XmlEscape(values[i]) + "</val>\n";
            request_xml += "</values>\n";
            return this;
        }

        public NOSQLServerCommand update(String table_name, String[] columns)
        {
            request_xml += "<command>update</command>\n";
            request_xml += "<update>\n";
            request_xml += "<table_name>" + XmlEscape(table_name) + "</table_name>\n";
            for (int i = 0; i < columns.Length; ++i)
                request_xml += "<col>" + XmlEscape(columns[i]) + "</col>\n";
            request_xml += "</update>\n";
            return this;
        }

        public NOSQLServerCommand delete(String table_name)
        {
            request_xml += "<command>delete</command>\n";
            request_xml += "<delete>\n";
            request_xml += "<table_name>" + XmlEscape(table_name) + "</table_name>\n";
            request_xml += "</delete>\n";
            return this;
        }

        public NOSQLServerCommand createdatabase(String new_database_name, String new_user_password)
        {
            request_xml += "<command>createdatabase</command>\n";
            request_xml += "<createdatabase>\n";
            request_xml += "<database_name>" + XmlEscape(new_database_name) + "</database_name>\n";
            request_xml += "<user_password>" + XmlEscape(new_user_password) + "</user_password>\n";
            request_xml += "</createdatabase>\n";
            return this;
        }

        public NOSQLServerCommand changepassword(String user_password)
        {
            request_xml += "<command>changepassword</command>\n";
            request_xml += "<changepassword>\n";
            request_xml += "<user_password>" + XmlEscape(user_password) + "</user_password>\n";
            request_xml += "</changepassword>\n";
            return this;
        }

        public NOSQLServerCommand showdatabases()
        {
            request_xml += "<command>showdatabases</command>\n";
            return this;
        }

        public NOSQLServerCommand showtables()
        {
            request_xml += "<command>showtables</command>\n";
            return this;
        }

        public NOSQLServerCommand showtable(String table_name)
        {
            request_xml += "<command>showtable</command>\n";
            request_xml += "<showtable>\n";
            request_xml += "<table_name>" + XmlEscape(table_name) + "</table_name>\n";
            request_xml += "</showtable>\n";
            return this;
        }

        public NOSQLServerCommand showindexes(String table_name)
        {
            request_xml += "<command>showindexes</command>\n";
            request_xml += "<showindexes>\n";
            request_xml += "<table_name>" + XmlEscape(table_name) + "</table_name>\n";
            request_xml += "</showindexes>\n";
            return this;
        }

        public NOSQLServerCommand showindex(String index_name)
        {
            request_xml += "<command>showindex</command>\n";
            request_xml += "<showindex>\n";
            request_xml += "<index_name>" + XmlEscape(index_name) + "</index_name>\n";
            request_xml += "</showindex>\n";
            return this;
        }

        public NOSQLServerCommand deletetable(String table_name)
        {
            request_xml += "<command>deletetable</command>\n";
            request_xml += "<deletetable>\n";
            request_xml += "<table_name>" + XmlEscape(table_name) + "</table_name>\n";
            request_xml += "</deletetable>\n";
            return this;
        }
        public NOSQLServerCommand deletedatabase()
        {
            request_xml += "<command>deletedatabase</command>\n";
            return this;
        }

        public NOSQLServerCommand isdbexist()
        {
            request_xml += "<command>isdbexist</command>\n";
            return this;
        }
        public bool execute()
        {
            request_xml += "<db>" + XmlEscape(db) + "</db>\n";
            request_xml += "<password>" + XmlEscape(password) + "</password>\n";

            request_xml += "</root>";
            request_xml = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n" + request_xml;

            reset();
            
            TcpClient clientSocket = new TcpClient();
            try
            {
                clientSocket.Connect(server_ip, port);
            }
            catch (Exception e)
            {
                request_xml = "";
                message = e.Message;
                return false;
            }

            NetworkStream serverStream = null;

            try
            {
                serverStream = clientSocket.GetStream();
                byte[] outStream = Encoding.Unicode.GetBytes(request_xml);
                serverStream.Write(outStream, 0, outStream.Length);
                serverStream.Flush();
            }
            catch (Exception ex) {
                request_xml = "";
                message = ex.Message;
                return false;
            }

            request_xml = "";
            string returndata = "";

            try
            {
                int len = serverStream.Read(inStream, 0, 1000000);
                returndata = Encoding.Unicode.GetString(inStream, 0, len);
            }
            catch (Exception ex)
            {
                message = ex.Message;
                return false;
            }

            XmlDocument doc = new XmlDocument();
            try
            {
                doc.LoadXml(returndata);
            }
            catch (Exception ex)
            {
                message = ex.Message;
                return false;
            }

            message = doc.SelectNodes("//root/message")[0].InnerText;

            Int64 parsed = 0;
            hasrows = false;

            if (message != "OK" && Int64.TryParse(message, out parsed) == false)
                return false;

            if (message != "OK")
            {
                last_insert_id_or_affected_rows = parsed;
                message = "OK";
                return true;
            }
            else
                last_insert_id_or_affected_rows = 0;

            return true;
        }

        public NOSQLServerDataReader execute_reader() {
            var ret = new NOSQLServerDataReader();

            request_xml += "<db>" + XmlEscape(db) + "</db>\n";
            request_xml += "<password>" + XmlEscape(password) + "</password>\n";

            request_xml += "</root>";
            request_xml = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n" + request_xml;

            reset();

            TcpClient clientSocket = new TcpClient();
            try
            {
                clientSocket.Connect(server_ip, port);
            }
            catch (Exception e)
            {
                request_xml = "";
                ret.message = message = e.Message;
                return null;
            }

            NetworkStream serverStream = null;

            try
            {
                serverStream = clientSocket.GetStream();
                byte[] outStream = Encoding.Unicode.GetBytes(request_xml);
                serverStream.Write(outStream, 0, outStream.Length);
                serverStream.Flush();
            }
            catch (Exception ex)
            {
                request_xml = "";
                ret.message = message = ex.Message;
                return null;
            }

            request_xml = "";
            string returndata = "";

            try
            {
                int len = serverStream.Read(inStream, 0, 1000000);
                returndata = Encoding.Unicode.GetString(inStream, 0, len);
            }
            catch (Exception ex)
            {
                ret.message = message = ex.Message;
                return null;
            }

            XmlDocument doc = new XmlDocument();
            try
            {
                doc.LoadXml(returndata);
            }
            catch (Exception ex)
            {
                ret.message = message = ex.Message;
                return null;
            }

            message = doc.SelectNodes("//root/message")[0].InnerText;
            var rid = doc.SelectNodes("//root/rid");

            if (rid.Count > 0 && Int64.TryParse(rid[0].InnerText, out ret.recordset_id))
                ret.hasrows = true;
            else
            {
                ret.recordset_id = 0;
                ret.hasrows = false;
            }

            ret.db = db;
            ret.password = password;
            ret.port = port;
            ret.server_ip = server_ip;
            ret.message = message;

            return ret;
        }
        private void reset()
        {
            colnames = new List<String>();
            types = new List<String>();
            records = new List<List<String>>();
            message = "";
            hasrows = false;
            last_insert_id_or_affected_rows = 0;
        }
    }

    public class NOSQLServerDataReader
    {
        public Int64 recordset_id;
        public String db, password, server_ip;
        public int port;
        public String request_xml = "";

        public List<String> colnames;
        public List<String> types;
        public List<String> record;
        public String message;
        public Boolean hasrows;

        private static byte[] inStream = new byte[1000000];

        public bool fetch_next_record()
        {
            var ret = new List<String>();
            request_xml = "<command>RECORDSET-FETCH-RESULT</command>\n";
            request_xml += "<recordset_id>" + XmlEscape(recordset_id.ToString()) + "</recordset_id>\n";

            request_xml += "<db>" + XmlEscape(db) + "</db>\n";
            request_xml += "<password>" + XmlEscape(password) + "</password>\n";

            request_xml += "</root>";
            request_xml = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n" + request_xml;

            reset();

            TcpClient clientSocket = new TcpClient();
            try
            {
                clientSocket.Connect(server_ip, port);
            }
            catch (Exception e)
            {
                request_xml = "";
                message = e.Message;
                return false;
            }

            NetworkStream serverStream = null;

            try
            {
                serverStream = clientSocket.GetStream();
                byte[] outStream = Encoding.Unicode.GetBytes(request_xml);
                serverStream.Write(outStream, 0, outStream.Length);
                serverStream.Flush();
            }
            catch (Exception ex)
            {
                request_xml = "";
                message = ex.Message;
                return false;
            }

            request_xml = "";
            string returndata = "";

            try
            {
                int len = serverStream.Read(inStream, 0, 1000000);
                returndata = Encoding.Unicode.GetString(inStream, 0, len);
            }
            catch (Exception ex)
            {
                message = ex.Message;
                return false;
            }

            XmlDocument doc = new XmlDocument();
            try
            {
                doc.LoadXml(returndata);
            }
            catch (Exception ex)
            {
                message = ex.Message;
                return false;
            }

            message = doc.SelectNodes("//root/message")[0].InnerText;
            hasrows = doc.SelectNodes("//root/hasrows")[0].InnerText == "1";

            if (hasrows)
            { 
                XmlNodeList recs0 = doc.SelectNodes("//root/records");
                XmlNodeList recs = recs0.Item(0).ChildNodes;

                XmlNodeList col = recs.Item(0).ChildNodes;
                List<String> rec = new List<String>();

                for (int k = 0; k < col.Count; ++k)
                    record.Add(col.Item(k).InnerText);

                return true;
            }

            return false;
        }

        public bool fetch_fields()
        {
            var ret = new List<String>();
            request_xml = "<command>RECORDSET-FETCH-FIELDS</command>\n";
            request_xml += "<recordset_id>" + XmlEscape(recordset_id.ToString()) + "</recordset_id>\n";

            request_xml += "<db>" + XmlEscape(db) + "</db>\n";
            request_xml += "<password>" + XmlEscape(password) + "</password>\n";

            request_xml += "</root>";
            request_xml = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n" + request_xml;

            reset2();

            TcpClient clientSocket = new TcpClient();
            try
            {
                clientSocket.Connect(server_ip, port);
            }
            catch (Exception e)
            {
                request_xml = "";
                message = e.Message;
                return false;
            }

            NetworkStream serverStream = null;

            try
            {
                serverStream = clientSocket.GetStream();
                byte[] outStream = Encoding.Unicode.GetBytes(request_xml);
                serverStream.Write(outStream, 0, outStream.Length);
                serverStream.Flush();
            }
            catch (Exception ex)
            {
                request_xml = "";
                message = ex.Message;
                return false;
            }

            request_xml = "";
            string returndata = "";

            try
            {
                int len = serverStream.Read(inStream, 0, 1000000);
                returndata = Encoding.Unicode.GetString(inStream, 0, len);
            }
            catch (Exception ex)
            {
                message = ex.Message;
                return false;
            }

            XmlDocument doc = new XmlDocument();
            try
            {
                doc.LoadXml(returndata);
            }
            catch (Exception ex)
            {
                message = ex.Message;
                return false;
            }

            message = doc.SelectNodes("//root/message")[0].InnerText;
            hasrows = doc.SelectNodes("//root/hasrows")[0].InnerText == "1";

            if (message == "OK")
            {
                XmlNodeList cols0 = doc.SelectNodes("//root/colnames");
                XmlNodeList cols = cols0.Item(0).ChildNodes;

                for (int i = 0; i < cols.Count; ++i)
                    colnames.Add(cols.Item(i).InnerText);

                XmlNodeList typs0 = doc.SelectNodes("//root/types");
                XmlNodeList typs = typs0.Item(0).ChildNodes;

                for (int i = 0; i < typs.Count; ++i)
                    types.Add(typs.Item(i).InnerText);

                return true;
            }

            return false;
        }

        public int num_rows()
        {
            request_xml = "<command>RECORDSET-NUM-ROWS</command>\n";
            request_xml += "<recordset_id>" + XmlEscape(recordset_id.ToString()) + "</recordset_id>\n";

            request_xml += "<db>" + XmlEscape(db) + "</db>\n";
            request_xml += "<password>" + XmlEscape(password) + "</password>\n";

            request_xml += "</root>";
            request_xml = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n" + request_xml;

            message = "";
            hasrows = false;

            TcpClient clientSocket = new TcpClient();
            try
            {
                clientSocket.Connect(server_ip, port);
            }
            catch (Exception e)
            {
                request_xml = "";
                message = e.Message;
                return -1;
            }

            NetworkStream serverStream = null;

            try
            {
                serverStream = clientSocket.GetStream();
                byte[] outStream = Encoding.Unicode.GetBytes(request_xml);
                serverStream.Write(outStream, 0, outStream.Length);
                serverStream.Flush();
            }
            catch (Exception ex)
            {
                request_xml = "";
                message = ex.Message;
                return -1;
            }

            request_xml = "";
            string returndata = "";

            try
            {
                int len = serverStream.Read(inStream, 0, 1000000);
                returndata = Encoding.Unicode.GetString(inStream, 0, len);
            }
            catch (Exception ex)
            {
                message = ex.Message;
                return -1;
            }

            XmlDocument doc = new XmlDocument();
            try
            {
                doc.LoadXml(returndata);
            }
            catch (Exception ex)
            {
                message = ex.Message;
                return -1;
            }

            message = doc.SelectNodes("//root/message")[0].InnerText;
            int i32;

            if (int.TryParse(message, out i32))
            {
                message = "OK";
                return i32;
            }

            return -1;
        }

        public bool move_first()
        {
            request_xml = "<command>RECORDSET-MOVE-FIRST</command>\n";
            request_xml += "<recordset_id>" + XmlEscape(recordset_id.ToString()) + "</recordset_id>\n";

            request_xml += "<db>" + XmlEscape(db) + "</db>\n";
            request_xml += "<password>" + XmlEscape(password) + "</password>\n";

            request_xml += "</root>";
            request_xml = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n" + request_xml;

            reset();

            TcpClient clientSocket = new TcpClient();
            try
            {
                clientSocket.Connect(server_ip, port);
            }
            catch (Exception e)
            {
                request_xml = "";
                message = e.Message;
                return false;
            }

            NetworkStream serverStream = null;

            try
            {
                serverStream = clientSocket.GetStream();
                byte[] outStream = Encoding.Unicode.GetBytes(request_xml);
                serverStream.Write(outStream, 0, outStream.Length);
                serverStream.Flush();
            }
            catch (Exception ex)
            {
                request_xml = "";
                message = ex.Message;
                return false;
            }

            request_xml = "";
            string returndata = "";

            try
            {
                int len = serverStream.Read(inStream, 0, 1000000);
                returndata = Encoding.Unicode.GetString(inStream, 0, len);
            }
            catch (Exception ex)
            {
                message = ex.Message;
                return false;
            }

            XmlDocument doc = new XmlDocument();
            try
            {
                doc.LoadXml(returndata);
            }
            catch (Exception ex)
            {
                message = ex.Message;
                return false;
            }

            message = doc.SelectNodes("//root/message")[0].InnerText;
            
            if (message == "OK")
                return true;
            
            return false;
        }

        public bool close()
        {
            request_xml = "<command>RECORDSET-CLOSE</command>\n";
            request_xml += "<recordset_id>" + XmlEscape(recordset_id.ToString()) + "</recordset_id>\n";

            request_xml += "<db>" + XmlEscape(db) + "</db>\n";
            request_xml += "<password>" + XmlEscape(password) + "</password>\n";

            request_xml += "</root>";
            request_xml = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n" + request_xml;

            reset();

            TcpClient clientSocket = new TcpClient();
            try
            {
                clientSocket.Connect(server_ip, port);
            }
            catch (Exception e)
            {
                request_xml = "";
                message = e.Message;
                return false;
            }

            NetworkStream serverStream = null;

            try
            {
                serverStream = clientSocket.GetStream();
                byte[] outStream = Encoding.Unicode.GetBytes(request_xml);
                serverStream.Write(outStream, 0, outStream.Length);
                serverStream.Flush();
            }
            catch (Exception ex)
            {
                request_xml = "";
                message = ex.Message;
                return false;
            }

            request_xml = "";
            string returndata = "";

            try
            {
                int len = serverStream.Read(inStream, 0, 1000000);
                returndata = Encoding.Unicode.GetString(inStream, 0, len);
            }
            catch (Exception ex)
            {
                message = ex.Message;
                return false;
            }

            XmlDocument doc = new XmlDocument();
            try
            {
                doc.LoadXml(returndata);
            }
            catch (Exception ex)
            {
                message = ex.Message;
                return false;
            }

            message = doc.SelectNodes("//root/message")[0].InnerText;

            if (message == "OK")
                return true;

            return false;
        }

        private void reset()
        {
            record = new List<String>();
            message = "";
            hasrows = false;
        }

        private void reset2()
        {
            colnames = new List<String>();
            types = new List<String>();
            message = "";
            hasrows = false;
        }

        private string XmlEscape(string unescaped)
        {
            XmlDocument doc = new XmlDocument();
            XmlNode node = doc.CreateElement("root");
            node.InnerText = unescaped;
            return node.InnerXml;
        }

        private string XmlUnescape(string escaped)
        {
            XmlDocument doc = new XmlDocument();
            XmlNode node = doc.CreateElement("root");
            node.InnerXml = escaped;
            return node.InnerText;
        }
    }
}
