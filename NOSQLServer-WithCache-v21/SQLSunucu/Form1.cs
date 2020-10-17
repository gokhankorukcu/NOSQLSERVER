using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Diagnostics;
using System.Security.Cryptography;
using System.IO;
using System.Xml;
using System.Security.Permissions;
using EvalCSCodeNS;
using Ini;
using System.Runtime.InteropServices;
using System.Data.SQLite;


namespace SQLSunucu
{
    public partial class Form1 : Form
    {
        public static CultureInfo g_culture_info;
        public String cultureinfostr;

        int port;
        int Recordset_Activity_Timeout;

        Thread ana_thread;
        TcpListener serverSocket;
        TcpClient clientSocket = default(TcpClient);
        Boolean running = false;
        int client_counter = 0;
        
        Dictionary<String, ReaderWriterLockSlim> db_slots = new Dictionary<String, ReaderWriterLockSlim>();
        ReaderWriterLockSlim db_slots_rw_lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        public List<mytable> tables = new List<mytable>();
        public List<myindex> indexes = new List<myindex>();

        Random rnd = new Random((int)DateTime.Now.Ticks);

        public List<List<List<Object>>> curr_whole_records;
        public Boolean curr_ret;

        public Dictionary<String, CCacheSlot> cache_slots = new Dictionary<String, CCacheSlot>();
        ReaderWriterLockSlim cache_slots_rw_lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        Boolean recovery_is_running = false;
        public Form1()
        {
            if (PriorProcess() != null)
            {
                MessageBox.Show("Another instance of the app is already running!");
                this.Close();
                Environment.Exit(0);
                return;
            }
            InitializeComponent();

            var inf = new IniFile(Path.GetFullPath(".") +"\\config.ini");
            var portstr = inf.IniReadValue("GENERAL", "Port");
            cultureinfostr = inf.IniReadValue("GENERAL", "CultureInfo");
            port = Convert.ToInt32(portstr);
            Recordset_Activity_Timeout = Convert.ToInt32(inf.IniReadValue("GENERAL", "Recordset_Activity_Timeout"));
            inf = null;

            timer_refresh_slots.Interval = 1000 * Recordset_Activity_Timeout;
           
            g_culture_info = System.Globalization.CultureInfo.CreateSpecificCulture(cultureinfostr);

            g_culture_info.NumberFormat.NumberDecimalSeparator = ".";
            System.Threading.Thread.CurrentThread.CurrentCulture = g_culture_info;

            SQLiteFunction.RegisterFunction(typeof(SQLiteCaseInsensitiveCollation));
            SQLiteFunction.RegisterFunction(typeof(SQLiteCaseSensitiveCollation));
            SQLiteFunction.RegisterFunction(typeof(TOUPPER));
        }

        private void start()
        {
            for (var i = 0; i < tables.Count; ++i) {
                tables[i].columns_info = null;
            }
            for (var i = 0; i < indexes.Count; ++i)
            {
                indexes[i].sub_indexes = null;
            }

            tables = new List<mytable>();
            indexes = new List<myindex>();

            String[] allfiles = Directory.GetFiles("Databases", "*.ssd", SearchOption.AllDirectories);

            for (int i = 0; i < allfiles.Length; ++i)
            {
                if (allfiles[i].Split('.')[1] == "ssddup")
                    continue;
                String[] p = allfiles[i].Split('\\');
                String[] p2 = p[p.Length - 1].Split('.');
                String table_name = p2[0];
                String database_name = p[p.Length - 2];
                mytable my_table = new mytable();
                my_table.database_name = database_name;
                my_table.table_name = table_name;
                my_table.data_file_path = allfiles[i];
                p = allfiles[i].Split('.');
                my_table.form_file_path = p[0] + ".ssf";
                my_table.insert_log_file_path = p[0] + ".logins";
                my_table.update_log_file_path = p[0] + ".logupd";
                my_table.delete_log_file_path = p[0] + ".logdel";

                String table_form_content = File.ReadAllText(my_table.form_file_path);
                String[] form_sections = table_form_content.Split(new String[] { "\r\n-----\r\n" }, StringSplitOptions.None);
                String[] columns = form_sections[0].Split(new String[] { "\r\n" }, StringSplitOptions.None);
                for (var k = 0; k < columns.Length; ++k)
                {
                    String[] cp = columns[k].Split(' ');
                    AlanInfo alaninfo = new AlanInfo();
                    alaninfo.name = cp[0];
                    alaninfo.type = cp[1];
                    alaninfo.ai = cp.Length == 3 && cp[2] == "IDENTITY" ? true : false;
                    alaninfo.ci = cp.Length == 3 && cp[2] == "CI" ? true : false;
                    my_table.columns_info.Add(alaninfo);
                }
                tables.Add(my_table);

                if (form_sections.Length > 1)
                {
                    String[] indexdefs = form_sections[1].Split(new String[] { "\r\n" }, StringSplitOptions.None);
                    for (var k = 0; k < indexdefs.Length; ++k)
                    {
                        String[] ip = indexdefs[k].Split(' ');
                        myindex my_index = new myindex();
                        my_index.unique = ip.Length > 1 ? true : false;
                        my_index.database_name = my_table.database_name;
                        my_index.table_name = my_table.table_name;
                        my_index.index_name = ip[0];
                        my_index.file_path = "Databases\\" + my_index.database_name + "\\" + ip[0] + ".indexdef";

                        var indexdef_file_content = "";
                        try
                        {
                            indexdef_file_content = File.ReadAllText(my_index.file_path);
                        }
                        catch (Exception e) {
                        }
                        String[] findexes = indexdef_file_content.Split(new String[] { "\r\n" }, StringSplitOptions.None);

                        for (var j = 0; j < findexes.Length; ++j)
                        {
                            String[] ci = findexes[j].Split(' ');
                            mysubindex sub_index = new mysubindex();
                            sub_index.column_name = ci[0];
                            sub_index.id = ci.Length == 1 ? "0" : ci[1];
                            sub_index.file_path = "Databases\\" + my_index.database_name + "\\" + sub_index.id + ".ndx";

                            //sub_index.index_file = new SQLiteConnection("Data Source=" + sub_index.file_path);

                            my_index.sub_indexes.Add(sub_index);
                        }

                        indexes.Add(my_index);
                    }
                }
            }
        }

        private void check_update_integrity()
        {
            //UPDATE loga bak
            for (var i = 0; i < tables.Count; ++i) {
            BASLANGIC:
                if (i >= tables.Count)
                    break;

                mytable curr_table = tables[i];
                Stream logfp;
                try
                {
                    logfp = File.Open(curr_table.update_log_file_path, FileMode.Open, FileAccess.Read, FileShare.None);
                }
                catch (Exception ex) {
                    continue;
                }
                Int64 record_count = 0;
                byte[] record_count_bytes = new byte[sizeof(Int64)];
                byte[] abyte = new byte[1];
                List<Int64> updated_poses = new List<Int64>();
                List<Int64> new_poses = new List<Int64>();
                Int64 pos = 0;
                byte[] pos_bytes = new byte[sizeof(Int64)];

                if (logfp.Read(abyte, 0, 1) != 1)
                {
                    logfp.Close();
                    logfp = File.Open(curr_table.update_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
                    logfp.Close();
                    continue;
                }

                if (logfp.Read(record_count_bytes, 0, sizeof(Int64)) != sizeof(Int64))
                {
                    logfp.Close();
                    logfp = File.Open(curr_table.update_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
                    logfp.Close();
                    continue;
                }

                record_count = BitConverter.ToInt64(record_count_bytes, 0);

                for (Int64 k = 0; k < record_count; ++k) {
                    if (logfp.Read(pos_bytes, 0, sizeof(Int64)) != sizeof(Int64))
                    {
                        logfp.Close();
                        logfp = File.Open(curr_table.update_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
                        logfp.Close();
                        ++i;
                        goto BASLANGIC;
                    }
                    pos = BitConverter.ToInt64(pos_bytes, 0);
                    updated_poses.Add(pos);
                }

                var failure = false;
                for (Int64 k = 0; k < record_count; ++k)
                {
                    if (logfp.Read(pos_bytes, 0, sizeof(Int64)) != sizeof(Int64))
                    {
                        failure = true;
                        break;
                    }
                    pos = BitConverter.ToInt64(pos_bytes, 0);
                    new_poses.Add(pos);
                }

                if (!failure)
                    if (logfp.Read(abyte, 0, 1) != 1)
                        failure = true;
                    

                if (failure) 
                {
                    var message = "Database may be corrupted! (Code:1) Recovery process will start now and this may be take a long time.";
                    if (!recovery_is_running && MessageBox.Show(message, "Error", MessageBoxButtons.OKCancel, MessageBoxIcon.Error) == DialogResult.Cancel)
                    {
                        logfp.Close();
                        this.Close();
                        Environment.Exit(0);
                        return;
                    }
                    recovery_is_running = true;
                    if (change_table(ref curr_table, ref updated_poses, ref new_poses) == false) {
                        logfp.Close();
                        this.Close();
                        Environment.Exit(0);
                        return;
                    }
                }

                logfp.Close();
                logfp = File.Open(curr_table.update_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
                logfp.Close();
                try
                {
                    if (failure)
                    {

                        File.Copy(curr_table.data_file_path + "dup", curr_table.data_file_path, true);
                        File.Delete(curr_table.data_file_path + "dup");
                    }
                }
                catch (Exception ex) { 
                
                }
                
            }
        }

        private void check_delete_integrity()
        {
            //UPDATE loga bak
            for (var i = 0; i < tables.Count; ++i)
            {
            BASLANGIC:
                if (i >= tables.Count)
                    break;

                mytable curr_table = tables[i];
                Stream logfp;
                try
                {
                    logfp = File.Open(curr_table.delete_log_file_path, FileMode.Open, FileAccess.Read, FileShare.None);
                }
                catch (Exception ex)
                {
                    continue;
                }
                Int64 record_count = 0;
                byte[] record_count_bytes = new byte[sizeof(Int64)];
                byte[] abyte = new byte[1];
                List<Int64> updated_poses = new List<Int64>();
                List<Int64> new_poses = new List<Int64>();
                Int64 pos = 0;
                byte[] pos_bytes = new byte[sizeof(Int64)];

                if (logfp.Read(abyte, 0, 1) != 1)
                {
                    logfp.Close();
                    logfp = File.Open(curr_table.delete_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
                    logfp.Close();
                    continue;
                }

                if (logfp.Read(record_count_bytes, 0, sizeof(Int64)) != sizeof(Int64))
                {
                    logfp.Close();
                    logfp = File.Open(curr_table.delete_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
                    logfp.Close();
                    continue;
                }

                record_count = BitConverter.ToInt64(record_count_bytes, 0);

                for (Int64 k = 0; k < record_count; ++k)
                {
                    if (logfp.Read(pos_bytes, 0, sizeof(Int64)) != sizeof(Int64))
                    {
                        logfp.Close();
                        logfp = File.Open(curr_table.delete_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
                        logfp.Close();
                        ++i;
                        goto BASLANGIC;
                    }
                    pos = BitConverter.ToInt64(pos_bytes, 0);
                    updated_poses.Add(pos);
                }

                var failure = false;
                
                if (logfp.Read(abyte, 0, 1) != 1)
                    failure = true;

                if (failure)
                {
                    var message = "Database may be corrupted! (Code:2) Recovery process will start now and this may be take a long time.";
                    if (!recovery_is_running && MessageBox.Show(message, "Error", MessageBoxButtons.OKCancel, MessageBoxIcon.Error) == DialogResult.Cancel)
                    {
                        logfp.Close();
                        this.Close();
                        Environment.Exit(0);
                        return;
                    }
                    recovery_is_running = true;
                    if (change_table(ref curr_table, ref updated_poses, ref new_poses) == false)
                    {
                        logfp.Close();
                        this.Close();
                        Environment.Exit(0);
                        return;
                    }
                }

                logfp.Close();
                logfp = File.Open(curr_table.delete_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
                logfp.Close();
                try
                {
                    if (failure)
                    {

                        File.Copy(curr_table.data_file_path + "dup", curr_table.data_file_path, true);
                        File.Delete(curr_table.data_file_path + "dup");
                    }
                }
                catch (Exception ex)
                {

                }

            }
        }

        private void check_insert_integrity()
        {
            //UPDATE loga bak
            for (var i = 0; i < tables.Count; ++i)
            {
            BASLANGIC:
                if (i >= tables.Count)
                    break;

                mytable curr_table = tables[i];
                Stream logfp;
                try
                {
                    logfp = File.Open(curr_table.insert_log_file_path, FileMode.Open, FileAccess.Read, FileShare.None);
                }
                catch (Exception ex)
                {
                    continue;
                }
                Int64 record_count = 0;
                byte[] record_count_bytes = new byte[sizeof(Int64)];
                byte[] abyte = new byte[1];
                List<Int64> updated_poses = new List<Int64>();
                List<Int64> new_poses = new List<Int64>();
                Int64 pos = 0;
                byte[] pos_bytes = new byte[sizeof(Int64)];

                if (logfp.Read(abyte, 0, 1) != 1)
                {
                    logfp.Close();
                    logfp = File.Open(curr_table.insert_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
                    logfp.Close();
                    continue;
                }

                if (logfp.Read(pos_bytes, 0, sizeof(Int64)) != sizeof(Int64))
                {
                    logfp.Close();
                    logfp = File.Open(curr_table.insert_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
                    logfp.Close();
                    continue;
                }

                pos = BitConverter.ToInt64(pos_bytes, 0);

                new_poses.Add(pos);

                var failure = false;

                if (logfp.Read(abyte, 0, 1) != 1)
                    failure = true;

                if (failure)
                {
                    var message = "Database may be corrupted! (Code:3) Recovery process will start now and this may be take a long time.";
                    if (!recovery_is_running && MessageBox.Show(message, "Error", MessageBoxButtons.OKCancel, MessageBoxIcon.Error) == DialogResult.Cancel)
                    {
                        logfp.Close();
                        this.Close();
                        Environment.Exit(0);
                        return;
                    }
                    recovery_is_running = true;
                    if (change_table(ref curr_table, ref updated_poses, ref new_poses) == false)
                    {
                        logfp.Close();
                        this.Close();
                        Environment.Exit(0);
                        return;
                    }
                }

                logfp.Close();
                logfp = File.Open(curr_table.insert_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
                logfp.Close();
                try
                {
                    if (failure)
                    {

                        File.Copy(curr_table.data_file_path + "dup", curr_table.data_file_path, true);
                        File.Delete(curr_table.data_file_path + "dup");
                    }
                }
                catch (Exception ex)
                {

                }

            }
        }

        private void check_table_creation_integrity()
        {
            var dbs = new List<String>();

            for (var i = 0; i < tables.Count; ++i)
                dbs.Add(tables[i].database_name);

            dbs = dbs.Distinct().ToList();

            for (var i = 0; i < dbs.Count; ++i)
            {
                String cnt;
                try
                {
                    cnt = File.ReadAllText("Databases/" + dbs[i] + "/table_creation.log");
                }
                catch (Exception ex)
                {
                    return;
                }

                var failure = false;

                var p = cnt.Split(new String[] { "\r\n" }, StringSplitOptions.None);

                if (p.Length == 1)
                    continue;
                else if (p.Length == 2)
                    failure = true;

                if (failure)
                {
                    var message = "Database may be corrupted! (Code:6) Recovery process will start now and this may be take a long time.";
                    if (!recovery_is_running && MessageBox.Show(message, "Error", MessageBoxButtons.OKCancel, MessageBoxIcon.Error) == DialogResult.Cancel)
                    {
                        this.Close();
                        Environment.Exit(0);
                        return;
                    }
                    recovery_is_running = true;

                    try
                    {
                        File.Delete("Databases/" + dbs[i] + "/" + p[0] + ".ssd");
                        File.Delete("Databases/" + dbs[i] + "/" + p[0] + ".ssf");
                        File.Delete("Databases/" + dbs[i] + "/" + p[0] + ".logins");
                        File.Delete("Databases/" + dbs[i] + "/" + p[0] + ".logupd");
                        File.Delete("Databases/" + dbs[i] + "/" + p[0] + ".logdel");
                        File.Delete("Databases/" + dbs[i] + "/table_creation.log");
                    }
                    catch (Exception e)
                    {
                    }
                }
            }
        }

        private void check_password_change_integrity()
        {
            var dbs = new List<String>();

            for (var i = 0; i < tables.Count; ++i)
                dbs.Add(tables[i].database_name);

            dbs = dbs.Distinct().ToList();

            for (var i = 0; i < dbs.Count; ++i)
            {
                String cnt1;
                try
                {
                    cnt1 = File.ReadAllText("Databases/" + dbs[i] + "/password_change.log");
                }
                catch (Exception ex)
                {
                    return;
                }

                var failure1 = false;

                var p1 = cnt1.Split(new String[] { "\r\n" }, StringSplitOptions.None);

                if (p1.Length == 1)
                    continue;
                else if (p1.Length == 2)
                    failure1 = true;

                if (failure1)
                {
                    var message = "Database may be corrupted! (Code:16) Recovery process will start now and this may be take a long time.";
                    if (!recovery_is_running && MessageBox.Show(message, "Error", MessageBoxButtons.OKCancel, MessageBoxIcon.Error) == DialogResult.Cancel)
                    {
                        this.Close();
                        Environment.Exit(0);
                        return;
                    }
                    recovery_is_running = true;

                    try
                    {
                        File.Copy("Databases/" + dbs[i] + "/password_backup.bin", "Databases/" + dbs[i] + "/password.bin", true);
                        File.Delete("Databases/" + dbs[i] + "/password_change.log");
                    }
                    catch (Exception e)
                    {
                    }
                }
            }

            for (var i = 0; i < dbs.Count; ++i)
            {
                String cnt2;
                try
                {
                    cnt2 = File.ReadAllText("Databases/" + dbs[i] + "/password_backup_change.log");
                }
                catch (Exception ex)
                {
                    return;
                }

                var failure2 = false;

                var p2 = cnt2.Split(new String[] { "\r\n" }, StringSplitOptions.None);

                if (p2.Length == 1)
                    continue;
                else if (p2.Length == 2)
                    failure2 = true;

                if (failure2)
                {
                    var message = "Database may be corrupted! (Code:17) Recovery process will start now and this may be take a long time.";
                    if (!recovery_is_running && MessageBox.Show(message, "Error", MessageBoxButtons.OKCancel, MessageBoxIcon.Error) == DialogResult.Cancel)
                    {
                        this.Close();
                        Environment.Exit(0);
                        return;
                    }
                    recovery_is_running = true;

                    try
                    {
                        File.Copy("Databases/" + dbs[i] + "/password.bin", "Databases/" + dbs[i] + "/password_backup.bin", true);
                        File.Delete("Databases/" + dbs[i] + "/password_backup_change.log");
                    }
                    catch (Exception e)
                    {
                    }
                }
            }


            String cnt3;
            try
            {
                cnt3 = File.ReadAllText("Databases/password_change.log");
            }
            catch (Exception ex)
            {
                return;
            }

            var failure3 = false;

            var p3 = cnt3.Split(new String[] { "\r\n" }, StringSplitOptions.None);

            if (p3.Length == 2)
                failure3 = true;

            if (failure3)
            {
                var message = "Database may be corrupted! (Code:18) Recovery process will start now and this may be take a long time.";
                if (!recovery_is_running && MessageBox.Show(message, "Error", MessageBoxButtons.OKCancel, MessageBoxIcon.Error) == DialogResult.Cancel)
                {
                    this.Close();
                    Environment.Exit(0);
                    return;
                }
                recovery_is_running = true;

                try
                {
                    File.Copy("Databases/password_backup.bin", "Databases/password.bin", true);
                    File.Delete("Databases/password_change.log");
                }
                catch (Exception e)
                {
                }
            }

            String cnt4;
            try
            {
                cnt4 = File.ReadAllText("Databases/password_backup_change.log");
            }
            catch (Exception ex)
            {
                return;
            }

            var failure4 = false;

            var p4 = cnt4.Split(new String[] { "\r\n" }, StringSplitOptions.None);

            if (p4.Length == 2)
                failure4 = true;

            if (failure4)
            {
                var message = "Database may be corrupted! (Code:19) Recovery process will start now and this may be take a long time.";
                if (!recovery_is_running && MessageBox.Show(message, "Error", MessageBoxButtons.OKCancel, MessageBoxIcon.Error) == DialogResult.Cancel)
                {
                    this.Close();
                    Environment.Exit(0);
                    return;
                }
                recovery_is_running = true;

                try
                {
                    File.Copy("Databases/password.bin", "Databases/password_backup.bin", true);
                    File.Delete("Databases/password_backup_change.log");
                }
                catch (Exception e)
                {
                }
            }
        }
        private void check_table_deletion_integrity()
        {
            var dbs = new List<String>();

            for (var i = 0; i < tables.Count; ++i)
                dbs.Add(tables[i].database_name);

            dbs = dbs.Distinct().ToList();

            for (var i = 0; i < dbs.Count; ++i)
            {
                String cnt;
                try
                {
                    cnt = File.ReadAllText("Databases/" + dbs[i] + "/table_deletion.log");
                }
                catch (Exception ex)
                {
                    return;
                }

                var failure = false;

                var p = cnt.Split(new String[] { "\r\n" }, StringSplitOptions.None);

                if (p.Length == 1)
                    continue;
                else if (p.Length == 2)
                    failure = true;

                if (failure)
                {
                    var message = "Database may be corrupted! (Code:6) Recovery process will start now and this may be take a long time.";
                    if (!recovery_is_running && MessageBox.Show(message, "Error", MessageBoxButtons.OKCancel, MessageBoxIcon.Error) == DialogResult.Cancel)
                    {
                        this.Close();
                        Environment.Exit(0);
                        return;
                    }
                    recovery_is_running = true;

                    try
                    {
                        File.Delete("Databases/" + dbs[i] + "/" + p[0] + ".ssd");
                        File.Delete("Databases/" + dbs[i] + "/" + p[0] + ".ssf");
                        File.Delete("Databases/" + dbs[i] + "/" + p[0] + ".logins");
                        File.Delete("Databases/" + dbs[i] + "/" + p[0] + ".logupd");
                        File.Delete("Databases/" + dbs[i] + "/" + p[0] + ".logdel");
                        File.Delete("Databases/" + dbs[i] + "/table_deletion.log");
                    }
                    catch (Exception e)
                    {
                    }
                }
            }
        }

        private void check_db_creation_integrity()
        {
            //UPDATE loga bak

            String cnt;

            try
            {
                cnt = File.ReadAllText("Databases/database_creation.log");
            }
            catch (Exception ex)
            {
                return;
            }

            var failure = false;

            var p = cnt.Split(new String[] { "\r\n" }, StringSplitOptions.None);

            if (p.Length == 1)
                return;
            else if (p.Length == 2)
                failure = true;

            if (failure)
            {
                var message = "Database may be corrupted! (Code:7) Recovery process will start now and this may be take a long time.";
                if (!recovery_is_running && MessageBox.Show(message, "Error", MessageBoxButtons.OKCancel, MessageBoxIcon.Error) == DialogResult.Cancel)
                {
                    this.Close();
                    Environment.Exit(0);
                    return;
                }
                recovery_is_running = true;

                try
                {
                    Directory.Delete("Databases/" + p[0], true);
                    File.Delete("Databases/database_creation.log");
                }
                catch (Exception e) {
                }
            }
        }

        private void check_db_deletion_integrity()
        {
            //UPDATE loga bak

            String cnt;

            try
            {
                cnt = File.ReadAllText("Databases/database_deletion.log");
            }
            catch (Exception ex)
            {
                return;
            }

            var failure = false;

            var p = cnt.Split(new String[] { "\r\n" }, StringSplitOptions.None);

            if (p.Length == 1)
                return;
            else if (p.Length == 2)
                failure = true;

            if (failure)
            {
                var message = "Database may be corrupted! (Code:8) Recovery process will start now and this may be take a long time.";
                if (!recovery_is_running && MessageBox.Show(message, "Error", MessageBoxButtons.OKCancel, MessageBoxIcon.Error) == DialogResult.Cancel)
                {
                    this.Close();
                    Environment.Exit(0);
                    return;
                }
                recovery_is_running = true;

                try
                {
                    Directory.Delete("Databases/" + p[0], true);
                    File.Delete("Databases/database_deletion.log");
                }
                catch (Exception e)
                {
                }
            }
        }

        private void check_index_creation_integrity()
        {
            var dbs = new List<String>();

            for (var i = 0; i < tables.Count; ++i)
                dbs.Add(tables[i].database_name);

            dbs = dbs.Distinct().ToList();

            for (var i = 0; i < dbs.Count; ++i)
            {
                String cnt;
                try
                {
                    cnt = File.ReadAllText("Databases/" + dbs[i] + "/index_creation.log");
                }
                catch (Exception ex)
                {
                    return;
                }

                var failure = false;

                var p = cnt.Split(new String[] { "\r\n" }, StringSplitOptions.None);

                if (p.Length == 1)
                    continue;
                else if (p.Length == 2)
                    failure = true;

                if (failure)
                {
                    var message = "Database may be corrupted! (Code:10) Recovery process will start now and this may be take a long time.";
                    if (!recovery_is_running && MessageBox.Show(message, "Error", MessageBoxButtons.OKCancel, MessageBoxIcon.Error) == DialogResult.Cancel)
                    {
                        this.Close();
                        Environment.Exit(0);
                        return;
                    }
                    recovery_is_running = true;

                    try
                    {
                        var pp = p[0].Split(' ');

                        var table_form_content = File.ReadAllText("Databases/" + dbs[i] + "/" + pp[0] + ".ssf");

                        var cp = table_form_content.Split(new String[] { "\r\n-----\r\n" }, StringSplitOptions.None);

                        File.WriteAllText("Databases/" + dbs[i] + "/" + pp[0] + ".ssf", cp[0]);

                        if (cp.Length > 1)
                        {
                            var cpp = cp[1].Split(new String[] { "\r\n" }, StringSplitOptions.None);

                            var l_ndx = new List<String>();

                            for (var ii = 0; ii < cpp.Length; ++ii)
                            {
                                var sat = cpp[ii].Split(' ');

                                if (sat[0] != pp[1])
                                    l_ndx.Add(cpp[ii]);
                                else {
                                    if (File.Exists("Databases/" + dbs[i] + "/" + sat[0] + ".indexdef"))
                                    {
                                        var content = File.ReadAllText("Databases/" + dbs[i] + "/" + sat[0] + ".indexdef");
                                        var satirlar =  content.Split(new String[] { "\r\n" }, StringSplitOptions.None);
                                        for (var kk = 0; kk < satirlar.Length; ++kk)
                                        {
                                            var sp = satirlar[kk].Split(' ');
                                            if (sp.Length > 1 && sp[1].Length == 8)
                                            {
                                                File.Delete("Databases/" + dbs[i] + "/" + sp[1] + ".ndx");
                                            }
                                        }
                                    }    
                                }
                            }

                            var son_ndx = l_ndx[l_ndx.Count - 1].Split(' ')[0];
                            if (!File.Exists("Databases/" + dbs[i] + "/" + son_ndx + ".indexdef"))
                            {
                                l_ndx.RemoveAt(l_ndx.Count - 1);
                            }

                            if (l_ndx.Count > 0) {
                                File.AppendAllText("Databases/" + dbs[i] + "/" + pp[0] + ".ssf", "\r\n-----");
                                for (var ii = 0; ii < l_ndx.Count; ++ii)
                                {
                                    File.AppendAllText("Databases/" + dbs[i] + "/" + pp[0] + ".ssf", "\r\n"+ l_ndx[ii]);
                                }
                            }
                        }

                        File.Delete("Databases/" + dbs[i] + "/"+ pp[1] +".indexdef");
                        File.Delete("Databases/" + dbs[i] + "/index_creation.log");
                    }
                    catch (Exception e)
                    {
                    }
                }
            }
        }
        private Boolean change_table(ref mytable curr_table, ref List<Int64> updated_poses, ref List<Int64> new_poses) 
        {
            curr_whole_records = new List<List<List<Object>>>();

            var list_spreaded_select_columns = new List<select_column_struct>();
            
            for (var j = 0; j < curr_table.columns_info.Count; ++j)
            {
                select_column_struct spreaded_column = new select_column_struct();
                spreaded_column.ai = curr_table.columns_info[j].ai;
                spreaded_column.name = curr_table.columns_info[j].name;
                spreaded_column.type = curr_table.columns_info[j].type;
                spreaded_column.ci = curr_table.columns_info[j].ci;
                spreaded_column.table = curr_table;
                var curr_indexes = getindexes(curr_table.database_name, curr_table.table_name);
                for (var g = 0; g < curr_indexes.Count; ++g)
                    for (var s = 0; s < curr_indexes[g].sub_indexes.Count; ++s)
                        if (spreaded_column.name == curr_indexes[g].sub_indexes[s].column_name)
                        {
                            spreaded_column.index = curr_indexes[g].sub_indexes[s];
                            goto CIK;
                        }
            CIK:
                list_spreaded_select_columns.Add(spreaded_column);
            }

            
            Stream fp = File.Open(curr_table.data_file_path, FileMode.Open, FileAccess.Read, FileShare.None);

            int size = 0;
            var dosyasonu = false;
            Int64 identity_val = 0;
          
            while (true)
            {
                var curr_table_records = new List<List<Object>>();
                Int64 curr_pos = 0;

                while (true)
                {
                    curr_pos = fp.Position;

                    /*
                    if (new_poses.Count > 0 && new_poses[new_poses.Count - 1] == curr_pos)
                    {
                        dosyasonu = true;
                        break;
                    } */
                    var in_news = new_poses.Contains(curr_pos); 
                    var in_updates = updated_poses.Contains(curr_pos);

                    var curr_record = new List<Object>();
                    var deleted_flag = new byte[1];
                    if (fp.Read(deleted_flag, 0, 1) < 1)
                    {
                        dosyasonu = true;
                        goto WCIK;
                    }

                    var condition = (deleted_flag[0] == 0 && !in_news) || in_updates;

                    if (!condition)
                    {
                        for (var k = 0; k < curr_table.columns_info.Count; ++k)
                        {
                            var null_flag = new byte[1];
                            if (fp.Read(null_flag, 0, 1) != 1)
                            {
                                dosyasonu = true;
                                goto WCIK;
                            }
                            if (null_flag[0] == 1)
                            {
                                fp.Seek(1, SeekOrigin.Current);
                                continue;
                            }

                            switch (curr_table.columns_info[k].type)
                            {
                                case "tinyint": size = sizeof(sbyte); break;
                                case "smallint": size = sizeof(Int16); break;
                                case "int": size = sizeof(Int32); break;
                                case "bigint": size = sizeof(Int64); break;
                                case "text":
                                    var bytesofsize = new byte[sizeof(int)];
                                    if (fp.Read(bytesofsize, 0, sizeof(int)) != sizeof(int))
                                    {
                                        dosyasonu = true;
                                        goto WCIK;
                                    }
                                    size = BitConverter.ToInt32(bytesofsize, 0);
                                    break;
                                case "double": size = sizeof(double); break;
                            }
                            fp.Seek(size, SeekOrigin.Current);
                        }
                        continue;
                    }

                    var curr_column_value = new Object();
                    // deleted flag = 0, verileri çek

                    for (var j = 0; j < curr_table.columns_info.Count; ++j)
                    {
                        var null_flag = new byte[1];
                        if (fp.Read(null_flag, 0, 1) != 1)
                        {
                            dosyasonu = true;
                            goto WCIK;
                        }
                        if (null_flag[0] == 1)
                        {
                            fp.Seek(1, SeekOrigin.Current);
                            curr_record.Add(null);
                            continue;
                        }

                        byte[] curr_column_bytes;

                        switch (curr_table.columns_info[j].type)
                        {
                            case "tinyint":
                                size = sizeof(sbyte);
                                curr_column_bytes = new byte[size];
                                if (fp.Read(curr_column_bytes, 0, size) != size)
                                {
                                    dosyasonu = true;
                                    goto WCIK;
                                }
                                curr_column_value = curr_column_bytes[0];
                                break;
                            case "smallint":
                                size = sizeof(Int16);
                                curr_column_bytes = new byte[size];
                                if (fp.Read(curr_column_bytes, 0, size) != size)
                                {
                                    dosyasonu = true;
                                    goto WCIK;
                                }
                                curr_column_value = BitConverter.ToInt16(curr_column_bytes, 0);
                                break;
                            case "int": size =
                                sizeof(Int32);
                                curr_column_bytes = new byte[size];
                                if (fp.Read(curr_column_bytes, 0, size) != size)
                                {
                                    dosyasonu = true;
                                    goto WCIK;
                                }
                                curr_column_value = BitConverter.ToInt32(curr_column_bytes, 0);
                                break;
                            case "bigint":
                                size = sizeof(Int64);
                                curr_column_bytes = new byte[size];
                                if (fp.Read(curr_column_bytes, 0, size) != size)
                                {
                                    dosyasonu = true;
                                    goto WCIK;
                                }
                                curr_column_value = BitConverter.ToInt64(curr_column_bytes, 0);
                                break;
                            case "text":
                                var bytesofsize = new byte[sizeof(int)];
                                if (fp.Read(bytesofsize, 0, sizeof(int)) != sizeof(int))
                                {
                                    dosyasonu = true;
                                    goto WCIK;
                                }
                                size = BitConverter.ToInt32(bytesofsize, 0);

                                curr_column_bytes = new byte[size];
                                if (fp.Read(curr_column_bytes, 0, size) != size)
                                {
                                    dosyasonu = true;
                                    goto WCIK;
                                }

                                curr_column_value = Encoding.Unicode.GetString(curr_column_bytes, 0, size);
                                break;
                            case "double":
                                size = sizeof(double);
                                curr_column_bytes = new byte[size];
                                if (fp.Read(curr_column_bytes, 0, size) != size)
                                {
                                    dosyasonu = true;
                                    goto WCIK;
                                }
                                curr_column_value = BitConverter.ToDouble(curr_column_bytes, 0);
                                break;

                        }

                        for (var k = 0; k < list_spreaded_select_columns.Count; ++k)
                        {
                            if (curr_table.columns_info[j].name == list_spreaded_select_columns[k].name)
                            {
                                curr_record.Add(curr_column_value);
                                if (list_spreaded_select_columns[k].ai)
                                {
                                    if (Int64.Parse(curr_column_value.ToString()) > identity_val)
                                        identity_val = Int64.Parse(curr_column_value.ToString());
                                }
                            }
                        }
                    }

                    curr_record.Add(curr_pos);
                    curr_table_records.Add(curr_record);
                }

            WCIK:
                curr_whole_records.Add(curr_table_records);

                if (dosyasonu) 
                    break;
            }

            fp.Close();

            for (var i = 0; i < list_spreaded_select_columns.Count; ++i)
            {
                if (list_spreaded_select_columns[i].ai) {
                    File.WriteAllText("Databases/" + curr_table.database_name + "/" + curr_table.table_name + "." + list_spreaded_select_columns[i].name +".inc", identity_val.ToString());
                    break;
                }
            }
            
            Stream st = File.Open(curr_table.data_file_path +"dup", FileMode.Create, FileAccess.Write, FileShare.None);

            for (var w = 0; w < curr_whole_records[0].Count; ++w)
            {
                curr_whole_records[0][w][curr_whole_records[0][0].Count - 1] = st.Position;

                byte[] deletedflag = new byte[1];
                deletedflag[0] = 0;
                st.Write(deletedflag, 0, 1);

                for (var i = 0; i < curr_whole_records[0][w].Count - 1; ++i)
                {
                    size = 0;

                    var curr_value = "";
                    if (curr_whole_records[0][w][i] != null)
                    {
                        curr_value = curr_whole_records[0][w][i].ToString();
                    }
                    else
                        curr_value = null;

                    switch (list_spreaded_select_columns[i].type)
                    {
                        case "tinyint": size = sizeof(sbyte); break;
                        case "smallint": size = sizeof(Int16); break;
                        case "int": size = sizeof(Int32); break;
                        case "bigint": size = sizeof(Int64); break;
                        case "text":
                            if (curr_value != null)
                            {
                                size = Encoding.Unicode.GetBytes(curr_value).Count();
                            }
                            break;
                        case "double": size = sizeof(double); break;
                    }

                    byte[] bytes = new byte[size];

                    if (curr_value != null)
                        switch (list_spreaded_select_columns[i].type)
                        {
                            case "tinyint": bytes[0] = (byte)sbyte.Parse(curr_value); break;
                            case "smallint": bytes = BitConverter.GetBytes(Int16.Parse(curr_value)); break;
                            case "int": bytes = BitConverter.GetBytes(Int32.Parse(curr_value)); break;
                            case "bigint": bytes = BitConverter.GetBytes(Int64.Parse(curr_value)); break;
                            case "text": bytes = Encoding.Unicode.GetBytes(curr_value); break;
                            case "double": bytes = BitConverter.GetBytes(double.Parse(curr_value)); break;
                        }

                    byte[] nullflag = new byte[1];
                    if (curr_value == null)
                    {
                        nullflag[0] = 1;
                    }
                    else
                        nullflag[0] = 0;

                    st.Write(nullflag, 0, 1);

                    if (curr_value == null)
                    {
                        st.Write(nullflag, 0, 1); // rastgele değer
                    }
                    else
                    {
                        if (curr_value != null && list_spreaded_select_columns[i].type == "text") // string ise stringin uzunluğunu belirten header yaz
                        {
                            byte[] header = BitConverter.GetBytes(size);
                            st.Write(header, 0, sizeof(int));
                        }

                        st.Write(bytes, 0, size);
                    }
                }
            }

            st.Close();


            var dialog = new create_indexes(curr_table, this);
            dialog.ShowDialog();
            return curr_ret;
    }
        
        private void button1_Click(object sender, EventArgs e)
        {
            recovery_is_running = false;

            start();
            check_db_creation_integrity();

            start();
            check_db_deletion_integrity();

            start();
            check_table_creation_integrity();

            start();
            check_table_deletion_integrity();

            start();
            check_index_creation_integrity();

            start();
            check_update_integrity();
            check_delete_integrity();
            check_insert_integrity();
            check_password_change_integrity();

            for (var i = 0; i < indexes.Count; ++i)
            {
                for (var k = 0; k < indexes[i].sub_indexes.Count; ++k)
                {
                    indexes[i].sub_indexes[k].index_file = new SQLiteConnection("Data Source=" + indexes[i].sub_indexes[k].file_path);
                    indexes[i].sub_indexes[k].index_file.Open();
                }
            }

            var alldbs = Directory.GetDirectories("Databases");

            db_slots_rw_lock.EnterWriteLock();
            for (int i = 0; i < alldbs.Length; ++i)
            {
                var pdb = alldbs[i].Split('\\');
                db_slots.Add(pdb[pdb.Length - 1], new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion));
            }
            db_slots_rw_lock.ExitWriteLock();

            running = true;
            timer_refresh_slots.Enabled = true;
            ana_thread = new Thread(ana_fonksiyon);
            ana_thread.Start();
            timer1.Enabled = true;
            button1.Enabled = false;
            button2.Enabled = true;
            checkBox1.Enabled = false;
            label2.Text = "Status : Running";
        }

        void ana_fonksiyon() {
            serverSocket = new TcpListener(checkBox1.Checked ? IPAddress.Any : IPAddress.Loopback, port);

            serverSocket.Start();

            client_counter = 0;
            while (running)
            {
                //if (client_counter >= 2)
                //    continue;

                clientSocket = serverSocket.AcceptTcpClient();
                ++client_counter;

                Thread client_thread = new Thread(client_fonksiyon);
                client_thread.Start(clientSocket);
            }

            while (client_counter > 0)
                ;
            serverSocket.Stop();
        }

        void sonbaglanti(Object o)
        {
            running = false;

            try
            {
                TcpClient clientSocket = new TcpClient();
                clientSocket.Connect("127.0.0.1", port);

                NetworkStream serverStream = clientSocket.GetStream();
                byte[] outStream = Encoding.Unicode.GetBytes("12345");
                serverStream.Write(outStream, 0, outStream.Length);
                serverStream.Flush();

                byte[] inStream = new byte[1000];
                int len = serverStream.Read(inStream, 0, 1000);
                string returndata = Encoding.Unicode.GetString(inStream, 0, len);
                clientSocket.Close();
            }
            catch (Exception ex) {

            }
        }
        void client_fonksiyon(Object sock)
        {
            System.Globalization.CultureInfo customCulture = (System.Globalization.CultureInfo)System.Threading.Thread.CurrentThread.CurrentCulture.Clone();
            customCulture.NumberFormat.NumberDecimalSeparator = ".";
            System.Threading.Thread.CurrentThread.CurrentCulture = customCulture;

            TcpClient clientSocket = (TcpClient)sock;
            String db = "";
            Boolean dbpass = false;
            Boolean rootpass = false;

            while (true) {
                String dataFromClient = "";
                NetworkStream networkStream = null;
                Byte[] bytesFrom = new Byte[1000000];

                try
                {
                    networkStream = clientSocket.GetStream();
                    int len = networkStream.Read(bytesFrom, 0, 1000000);
                    dataFromClient = Encoding.Unicode.GetString(bytesFrom, 0, len);
                }
                catch (Exception ex) { 
                
                }

                //MessageBox.Show(dataFromClient);

                String serverResponse = "";

                if (dataFromClient.Length >= 5 && dataFromClient.Substring(0, 5) == "<?xml")
                {
                    XmlDocument doc = new XmlDocument();
                    doc.LoadXml(dataFromClient);

                    db = doc.SelectNodes("//root/db")[0].InnerText;
                    if (!Directory.Exists("Databases/" + db))
                    {
                        serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("Database does not exist!") + "</message><hasrows>0</hasrows></root>";
                    }
                    else 
                    {
                        String gelenpass = doc.SelectNodes("//root/password")[0].InnerText;

                        String root_filepass_md5 = File.ReadAllText("Databases/password.bin", Encoding.ASCII);
                        String db_filepass_md5 = "";
                        if (db != "")
                            db_filepass_md5 = File.ReadAllText("Databases/" + db + "/password.bin", Encoding.ASCII);

                        rootpass = dbpass = false;
                        if (MD5_encode(gelenpass) == root_filepass_md5)
                        {
                            rootpass = true;
                        }
                        else if (db_filepass_md5 != "" && MD5_encode(gelenpass) == db_filepass_md5)
                        {
                            dbpass = true;
                        }
                        
                        if (!dbpass && !rootpass)
                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("Invalid password!") + "</message><hasrows>0</hasrows></root>";
                        else
                        {
                            XmlNodeList clist = doc.SelectNodes("//root/command");
                            XmlNodeList ridlist = doc.SelectNodes("//root/recordset_id");

                            if (clist.Count > 1 || clist.Count == 0)
                                serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("Query must be contain only one command!") + "</message><hasrows>0</hasrows></root>";
                            else {
                                String command = clist[0].InnerText;

                                if (db == "" && command != "createdatabase") {
                                    serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("No database selected!") + "</message><hasrows>0</hasrows></root>";
                                    command = "";
                                }

                                var recordset_id = "";

                                if (ridlist.Count > 0)
                                    recordset_id = ridlist[0].InnerText;

                                String ret;

                                switch (command) {
                                    case "isdbexist":
                                        db_lock(db, 0, 0);
                                        var retis = f_isdbexist(db);
                                        db_lock(db, 1, 0);

                                        if (retis)
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("OK") + "</message><hasrows>0</hasrows></root>";
                                        else
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("Database does not exist!") + "</message><hasrows>0</hasrows></root>";
                                        break;

                                    case "RECORDSET-FETCH-RESULT":
                                        cache_slots_rw_lock.EnterReadLock();

                                        if (!cache_slots.ContainsKey(recordset_id))
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("Recordset has timedout!") + "</message><hasrows>0</hasrows></root>";
                                        else
                                        {
                                            var cs = cache_slots[recordset_id];
                                            cs.last_access_time = DateTime.Now;
                                            if (cs.curr_record_indis >= cs.records.Count)
                                                serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("RECORDSET REACHED TO END") + "</message><hasrows>0</hasrows></root>";
                                            else
                                            {
                                                serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>OK</message>\n<hasrows>1</hasrows>";
                                                serverResponse += "<records>\n";

                                                serverResponse += "<r>\n";

                                                for (var k = 0; k < cs.records[cs.curr_record_indis].Count; ++k)
                                                    if (cs.records[cs.curr_record_indis][k] == null)
                                                        serverResponse += "<c></c>";
                                                    else
                                                        serverResponse += "<c>" + XmlEscape(cs.records[cs.curr_record_indis][k].ToString()) + "</c>";

                                                serverResponse += "</r>\n";

                                                serverResponse += "</records>\n";

                                                serverResponse += "</root>";                     
                                                ++cs.curr_record_indis;
                                            }
                                        }
                                        cache_slots_rw_lock.ExitReadLock();
                                        break;
                                    case "RECORDSET-NUM-ROWS":
                                        cache_slots_rw_lock.EnterReadLock();

                                        if (!cache_slots.ContainsKey(recordset_id))
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("Recordset has timedout!") + "</message><hasrows>0</hasrows></root>";
                                        else
                                        {
                                            var cs = cache_slots[recordset_id];
                                            cs.last_access_time = DateTime.Now;
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape((cs.records.Count-2).ToString()) + "</message><hasrows>0</hasrows></root>";
                                        }
                                        cache_slots_rw_lock.ExitReadLock();
                                        break;
                                    case "RECORDSET-MOVE-FIRST":
                                        cache_slots_rw_lock.EnterReadLock();

                                        if (!cache_slots.ContainsKey(recordset_id))
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("Recordset has timedout!") + "</message><hasrows>0</hasrows></root>";
                                        else
                                        {
                                            var cs = cache_slots[recordset_id];
                                            cs.last_access_time = DateTime.Now;
                                            cs.curr_record_indis = 2;
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("OK") + "</message><hasrows>0</hasrows></root>";
                                        }
                                        cache_slots_rw_lock.ExitReadLock();
                                        break;

                                    case "RECORDSET-CLOSE":
                                        cache_slots_rw_lock.EnterWriteLock();

                                        if (!cache_slots.ContainsKey(recordset_id))
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("Recordset has timedout!") + "</message><hasrows>0</hasrows></root>";
                                        else
                                        {
                                            cache_slots.Remove(recordset_id);
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("OK") + "</message><hasrows>0</hasrows></root>";
                                        }
                                        cache_slots_rw_lock.ExitWriteLock();
                                        break;

                                    case "RECORDSET-FETCH-FIELDS":
                                        cache_slots_rw_lock.EnterReadLock();

                                        if (!cache_slots.ContainsKey(recordset_id))
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("Recordset has timedout!") + "</message><hasrows>0</hasrows></root>";
                                        else
                                        {
                                            var cs = cache_slots[recordset_id];
                                            cs.last_access_time = DateTime.Now;

                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>OK</message>\n<hasrows>1</hasrows>";
                                            serverResponse += "<colnames>\n";
                                            for (var i = 0; i < cs.records[0].Count; ++i)
                                                serverResponse += "<col>" + XmlEscape(cs.records[0][i].ToString()) + "</col>";
                                            serverResponse += "</colnames>\n";

                                            serverResponse += "<types>\n";
                                            for (var i = 0; i < cs.records[1].Count; ++i)
                                                serverResponse += "<col>" + XmlEscape(cs.records[1][i].ToString()) + "</col>";
                                            serverResponse += "</types>\n";

                                            serverResponse += "</root>";
                                        }
                                        cache_slots_rw_lock.ExitReadLock();
                                        break;

                                    case "select":
                                        db_lock(db, 0, 0);

                                        var list_ret = f_select(db, doc);

                                        db_lock(db, 1, 0);

                                        Int32 i32;

                                        if (Int32.TryParse(list_ret[0][0].ToString(), out i32))
                                        {
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("OK") + "</message><rid>"+ XmlEscape(i32.ToString()) + "</rid><hasrows>1</hasrows></root>";
                                        }
                                        else
                                        {
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape(list_ret[0][0].ToString()) + "</message><rid>YOK</rid><hasrows>0</hasrows></root>";
                                        }
                                        break;
                                    case "insertinto":
                                        db_lock(db, 0, 1);

                                        ret = f_insert_into(db, doc);

                                        db_lock(db, 1, 1);
                                        serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape(ret) + "</message><hasrows>0</hasrows></root>";
                                        break;
                                    case "update":
                                        db_lock(db, 0, 1);

                                        ret = f_update(db, doc);

                                        db_lock(db, 1, 1);
                                        serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape(ret) + "</message><hasrows>0</hasrows></root>";
                                        break;
                                    case "delete":
                                        db_lock(db, 0, 1);

                                        ret = f_delete(db, doc);

                                        db_lock(db, 1, 1);
                                        serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape(ret) + "</message><hasrows>0</hasrows></root>";
                                        break;
                                    case "createtable":
                                        db_lock(db, 0, 1);
                                        ret = f_create_table(db, doc);
                                        db_lock(db, 1, 1);
                                        serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape(ret) + "</message><hasrows>0</hasrows></root>";
                                        break;
                                    case "createindex":
                                        db_lock(db, 0, 1);

                                        ret = f_create_index(db, doc);

                                        db_lock(db, 1, 1);
                                        serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape(ret) + "</message><hasrows>0</hasrows></root>";
                                        break;
                                    case "showtable":
                                        db_lock(db, 0, 0);

                                        var retta = f_showtable(db, doc);

                                        db_lock(db, 1, 0);
                                        if (retta.Count == 0)
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("Table has been deleted!") + "</message><hasrows>0</hasrows></root>";
                                        else
                                        {
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>OK</message>\n<hasrows>1</hasrows>";
                                            serverResponse += "<colnames>\n";
                                            serverResponse += "<col>" + XmlEscape("Column") + "</col>";
                                            serverResponse += "</colnames>\n";

                                            serverResponse += "<types>\n";
                                            serverResponse += "<col>" + XmlEscape("text") + "</col>";
                                            serverResponse += "</types>\n";

                                            serverResponse += "<records>\n";
                                            for (var i = 0; i < retta.Count; ++i)
                                            {
                                                serverResponse += "<r>\n";

                                                serverResponse += "<c>" + XmlEscape(retta[i].ToString()) + "</c>";

                                                serverResponse += "</r>\n";
                                            }
                                            serverResponse += "</records>\n";

                                            serverResponse += "</root>";
                                        }




                                        break;
                                    case "showindex":
                                        db_lock(db, 0, 0);


                                        var reti = f_showindex(db, doc);

                                        db_lock(db, 1, 0);

                                        if (reti.Count == 0)
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("Index has been deleted!") + "</message><hasrows>0</hasrows></root>";
                                        else
                                        {
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>OK</message>\n<hasrows>1</hasrows>";
                                            serverResponse += "<colnames>\n";
                                            serverResponse += "<col>" + XmlEscape("ID and Column") + "</col>";
                                            serverResponse += "</colnames>\n";

                                            serverResponse += "<types>\n";
                                            serverResponse += "<col>" + XmlEscape("text") + "</col>";
                                            serverResponse += "</types>\n";

                                            serverResponse += "<records>\n";
                                            for (var i = 0; i < reti.Count; ++i)
                                            {
                                                serverResponse += "<r>\n";

                                                serverResponse += "<c>" + XmlEscape(reti[i].ToString()) + "</c>";

                                                serverResponse += "</r>\n";
                                            }
                                            serverResponse += "</records>\n";

                                            serverResponse += "</root>";
                                        }




                                        break;
                                    case "showindexes":
                                        db_lock(db, 0, 0);

                                        var retin = f_showindexes(db, doc);

                                        db_lock(db, 1, 0);
                                        if (retin.Count == 0)
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("There ara no indexes for this table!") + "</message><hasrows>0</hasrows></root>";
                                        else
                                        {
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>OK</message>\n<hasrows>1</hasrows>";
                                            serverResponse += "<colnames>\n";
                                            serverResponse += "<col>" + XmlEscape("Indexes") + "</col>";
                                            serverResponse += "</colnames>\n";

                                            serverResponse += "<types>\n";
                                            serverResponse += "<col>" + XmlEscape("text") + "</col>";
                                            serverResponse += "</types>\n";

                                            serverResponse += "<records>\n";
                                            for (var i = 0; i < retin.Count; ++i)
                                            {
                                                serverResponse += "<r>\n";

                                                serverResponse += "<c>" + XmlEscape(retin[i].ToString()) + "</c>";

                                                serverResponse += "</r>\n";
                                            }
                                            serverResponse += "</records>\n";

                                            serverResponse += "</root>";
                                        }




                                        break;
                                    case "showtables":
                                            db_lock(db, 0, 0);

                                            var rett = f_showtables(db);

                                            db_lock(db, 1, 0);

                                        if (rett.Count == 0)
                                                serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("There are no tables!") + "</message><hasrows>0</hasrows></root>";
                                            else
                                            {
                                                serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>OK</message>\n<hasrows>1</hasrows>";
                                                serverResponse += "<colnames>\n";
                                                serverResponse += "<col>" + XmlEscape("Tables") + "</col>";
                                                serverResponse += "</colnames>\n";

                                                serverResponse += "<types>\n";
                                                serverResponse += "<col>" + XmlEscape("text") + "</col>";
                                                serverResponse += "</types>\n";

                                                serverResponse += "<records>\n";
                                                for (var i = 0; i < rett.Count; ++i)
                                                {
                                                    serverResponse += "<r>\n";

                                                    serverResponse += "<c>" + XmlEscape(rett[i].ToString()) + "</c>";

                                                    serverResponse += "</r>\n";
                                                }
                                                serverResponse += "</records>\n";

                                                serverResponse += "</root>";
                                            }
                                        



                                        break;
                                    case "showdatabases":
                                        if (!rootpass)
                                        {
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("User has no permission for this operation!") + "</message><hasrows>0</hasrows></root>";
                                        }
                                        else
                                        {
                                            db_lock(db, 0, 0);

                                            var retd = f_showdatabases();

                                            db_lock(db, 1, 0);
                                            if (retd.Count == 0)
                                                serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("There are no databases!") + "</message><hasrows>0</hasrows></root>";
                                            else
                                            {
                                                serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>OK</message>\n<hasrows>1</hasrows>";
                                                serverResponse += "<colnames>\n";
                                                serverResponse += "<col>" + XmlEscape("Databases") + "</col>";
                                                serverResponse += "</colnames>\n";

                                                serverResponse += "<types>\n";
                                                serverResponse += "<col>" + XmlEscape("text") + "</col>";
                                                serverResponse += "</types>\n";

                                                serverResponse += "<records>\n";
                                                for (var i = 0; i < retd.Count; ++i)
                                                {
                                                    serverResponse += "<r>\n";

                                                    serverResponse += "<c>" + XmlEscape(retd[i].ToString()) + "</c>";

                                                    serverResponse += "</r>\n";
                                                }
                                                serverResponse += "</records>\n";

                                                serverResponse += "</root>";
                                            }
                                        }
                                      
                                        break;
                                    case "createdatabase":
                                        if (!rootpass)
                                            ret = "User cannot meet required permission!";
                                        else
                                            ret = f_create_database(doc);
                                        serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape(ret) + "</message><hasrows>0</hasrows></root>";
                                        break;
                                    case "changepassword":
                                        db_lock(db, 0, 0);

                                        ret = f_change_password(db, doc);

                                        db_lock(db, 1, 0);
                                        serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape(ret) + "</message><hasrows>0</hasrows></root>";
                                        break;
                                    case "deletetable":
                                        db_lock(db, 0, 1);

                                        ret = f_delete_table(db, doc);

                                        db_lock(db, 1, 1);
                                        serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape(ret) + "</message><hasrows>0</hasrows></root>";
                                        break;
                                    case "deletedatabase":
                                        ret = f_delete_database(db);
                                        serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape(ret) + "</message><hasrows>0</hasrows></root>";
                                        break;

                                    default:
                                        if (db != "")
                                            serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("Invalid command!") + "</message><hasrows>0</hasrows></root>";
                                        break;

                                }
                            }
                        }
                    }

                    //serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>"+ XmlEscape("mesajim < çşğüıÇŞĞÜİ") +"</message>\n<colnames>\n<col>alanbir</col>\n<col>alaniki</col></colnames>\n<types></types>\n<records></records></root>";
                }
                else
                    serverResponse = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<root>\n<message>" + XmlEscape("Command must be in XML form!") + "</message><hasrows>0</hasrows></root>";

                Byte[] sendBytes = null;
                try
                {
                    sendBytes = Encoding.Unicode.GetBytes(serverResponse);
                    networkStream.Write(sendBytes, 0, sendBytes.Length);
                    networkStream.Flush();
                }
                catch (Exception ex) { 
                
                }
    
                break;
            }

            --client_counter;
        }

        mytable gettable(String pdb, String ptablename) {
            //tables_rw_lock.EnterReadLock();

            for (var i = 0; i < tables.Count; ++i)
                if (tables[i].database_name == pdb && tables[i].table_name == ptablename)
                {
                    //tables_rw_lock.ExitReadLock();
                    return tables[i];
                }
                    
            //tables_rw_lock.ExitReadLock();
            return null;
        }

        public List<myindex> getindexes(String pdb, String ptablename) {
            var l_indexes = new List<myindex>();

            //indexes_rw_lock.EnterReadLock();

            for (var i = 0; i < indexes.Count; ++i)
            {
                if (indexes[i].database_name == pdb && indexes[i].table_name == ptablename)
                {
                    l_indexes.Add(indexes[i]);
                }   
            }

            //indexes_rw_lock.ExitReadLock();
            return l_indexes;
        }

        String f_create_database(XmlDocument pdoc)
        {
            db_slots_rw_lock.EnterWriteLock();
            String database_name = pdoc.SelectNodes("//root/createdatabase/database_name")[0].InnerText;
            String user_password = pdoc.SelectNodes("//root/createdatabase/user_password")[0].InnerText;

            File.WriteAllText("Databases/database_creation.log", database_name + "\r\nB");

            if (Directory.Exists("Databases/" + database_name)) {
                File.Delete("Databases/database_creation.log");
                db_slots_rw_lock.ExitWriteLock();
                return "Database is already exist!";
            }

            Directory.CreateDirectory("Databases/" + database_name);

            File.WriteAllText("Databases/" + database_name + "/password.bin", MD5_encode(user_password));
            File.WriteAllText("Databases/" + database_name + "/password_backup.bin", MD5_encode(user_password));

            File.AppendAllText("Databases/database_creation.log", "\r\nE");

            db_slots.Add(database_name, new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion));

            db_slots_rw_lock.ExitWriteLock();
            return "OK";
        }

        String f_delete_database(String db)
        {

            db_slots_rw_lock.EnterWriteLock();

            File.WriteAllText("Databases/database_deletion.log", db + "\r\nB");

            if (!Directory.Exists("Databases/" + db))
            {
                File.Delete("Databases/database_deletion.log");
                db_slots_rw_lock.ExitWriteLock();
                return "Database does not exist!";
            }


            

BASLANGIC:
            for (var i = 0; i < tables.Count; ++i)
            {
                if (tables[i].database_name == db)
                {
                BASLANGIC2:
                    for (var k = 0; k < indexes.Count; ++k)
                    {
                        if (indexes[k].database_name == db && indexes[k].table_name == tables[i].table_name)
                        {
                            for (var j = 0; j < indexes[k].sub_indexes.Count; ++j)
                            {
                                
                                indexes[k].sub_indexes[j].index_file.Close();
                                indexes[k].sub_indexes[j].index_file.Dispose();
                                File.Delete(indexes[k].sub_indexes[j].file_path);
                               
                            }
                            File.Delete(indexes[k].file_path);
                            indexes.RemoveAt(k);
                            goto BASLANGIC2;
                        }
                    }

                    for (var k = 0; k < tables[i].columns_info.Count; ++k)
                    {
                        if (tables[i].columns_info[k].ai)
                        {
                            File.Delete("Databases/" + tables[i].database_name + "/" + tables[i].table_name + "." + tables[i].columns_info[k].name + ".inc");
                        }
                    }

                    
                    File.Delete(tables[i].data_file_path);
                    File.Delete(tables[i].data_file_path + "dup");
                    File.Delete(tables[i].form_file_path);
                    File.Delete(tables[i].insert_log_file_path);
                    File.Delete(tables[i].delete_log_file_path);
                    File.Delete(tables[i].update_log_file_path);
                    

                    tables.RemoveAt(i);
                    goto BASLANGIC;
                }
            }


            

            Directory.Delete("Databases/" + db, true);

            File.AppendAllText("Databases/database_deletion.log", "\r\nE");

            db_slots.Remove(db);

            db_slots_rw_lock.ExitWriteLock();

            return "OK";
        }

        String f_delete_table(String db, XmlDocument pdoc)
        {
            String tablename = pdoc.SelectNodes("//root/deletetable/table_name")[0].InnerText;

            

            File.WriteAllText("Databases/"+ db +"/table_deletion.log", tablename + "\r\nB");

            if (!Directory.Exists("Databases/" + db))
            {
                File.Delete("Databases/" + db + "/table_deletion.log");
                
                return "Database does not exist!";
            }

BASLANGIC:
            for (var i = 0; i < indexes.Count; ++i)
            {
                if (indexes[i].database_name == db && indexes[i].table_name == tablename)
                {
                    for (var k = 0; k < indexes[i].sub_indexes.Count; ++k)
                    {
                        
                        indexes[i].sub_indexes[k].index_file.Close();
                        indexes[i].sub_indexes[k].index_file.Dispose();
                        File.Delete(indexes[i].sub_indexes[k].file_path);
                        
                    }
                    File.Delete(indexes[i].file_path);
                    indexes.RemoveAt(i);
                    goto BASLANGIC;
                }
            }


            for (var i = 0; i < tables.Count; ++i)
            {
                if (tables[i].database_name == db && tables[i].table_name == tablename)
                {
                    for (var k = 0; k < tables[i].columns_info.Count; ++k)
                    {
                        if (tables[i].columns_info[k].ai) {
                            File.Delete("Databases/" + db + "/" + tablename + "." + tables[i].columns_info[k].name +".inc");
                        }
                    }

                    
                    File.Delete(tables[i].data_file_path);
                    File.Delete(tables[i].data_file_path +"dup");
                    File.Delete(tables[i].form_file_path);
                    File.Delete(tables[i].insert_log_file_path);
                    File.Delete(tables[i].delete_log_file_path);
                    File.Delete(tables[i].update_log_file_path);
                    

                    tables.RemoveAt(i);
                    break;
                }
            }

            File.AppendAllText("Databases/" + db + "/table_deletion.log", "\r\nE");

            
            return "OK";
        }

        String f_change_password(String db, XmlDocument pdoc)
        {
            String user_password = pdoc.SelectNodes("//root/changepassword/user_password")[0].InnerText;

           

            if (db != "" && !Directory.Exists("Databases/" + db))
            {
                
                return "Database does not exist!";
            }

            if (db != "")
                db += "/";

            File.WriteAllText("Databases/" + db + "password_change.log", "changed\r\nB");
            
            File.WriteAllText("Databases/" + db + "password.bin", MD5_encode(user_password));

            File.AppendAllText("Databases/" + db + "password_change.log", "\r\nE");

            File.WriteAllText("Databases/" + db + "password_backup_change.log", "changed\r\nB");

            File.Copy("Databases/" + db + "password.bin", "Databases/" + db + "password_backup.bin", true);

            File.AppendAllText("Databases/" + db + "password_backup_change.log", "\r\nE");

            
            return "OK";
        }

        List<String> f_showdatabases()
        {
            var ret = new List<String>();

            String[] alldirs = Directory.GetDirectories("Databases", "*", SearchOption.TopDirectoryOnly);

            for (var i = 0; i < alldirs.Count(); ++i)
            {
                var p = alldirs[i].Split('\\');

                ret.Add(p[p.Length-1]);
            }
            return ret;
        }

        Boolean f_isdbexist(String db)
        {
            
            if (!Directory.Exists("Databases/" + db))
            {
                
                return false;
            }
            
            return true;
        }
        List<String> f_showtables(String db)
        {
            var ret = new List<String>();

            

            for (var i = 0; i < tables.Count; ++i) {
                if (tables[i].database_name == db)
                    ret.Add(tables[i].table_name);
            } 

            

            return ret;
        }

        List<String> f_showtable(String db, XmlDocument pdoc)
        {
            var ret = new List<String>();
            String tablename = pdoc.SelectNodes("//root/showtable/table_name")[0].InnerText;

            

            for (var i = 0; i < tables.Count; ++i)
            {
                if (tables[i].database_name == db && tables[i].table_name == tablename) {
                    for (var k = 0; k < tables[i].columns_info.Count; ++k) {
                        var str = tables[i].columns_info[k].name + " " + tables[i].columns_info[k].type;
                        str += tables[i].columns_info[k].ai ? " IDENTITY" : "";
                        str += tables[i].columns_info[k].ci ? " CI" : "";
                        ret.Add(str);
                    }
                    break;
                }
            }

            

            return ret;
        }

        List<String> f_showindex(String db, XmlDocument pdoc)
        {
            var ret = new List<String>();
            String indexname = pdoc.SelectNodes("//root/showindex/index_name")[0].InnerText;

            

            for (var i = 0; i < indexes.Count; ++i)
            {
                if (indexes[i].database_name == db && indexes[i].index_name == indexname)
                {
                    for (var k = 0; k < indexes[i].sub_indexes.Count; ++k)
                    {
                        var str = indexes[i].sub_indexes[k].id + " " + indexes[i].sub_indexes[k].column_name;
                        ret.Add(str);
                    }
                    break;
                }
            }

            

            return ret;
        }

        List<String> f_showindexes(String db, XmlDocument pdoc)
        {
            var ret = new List<String>();
            String tablename = pdoc.SelectNodes("//root/showindexes/table_name")[0].InnerText;

            

            for (var i = 0; i < indexes.Count; ++i)
            {
                if (indexes[i].database_name == db && indexes[i].table_name == tablename)
                {

                    var str = indexes[i].index_name;
                    str += indexes[i].unique ? " UNIQUE" : "";
                    ret.Add(str);
                    
                }
            }

            

            return ret;
        }
        List<List<Object>> f_select(String db, XmlDocument pdoc)
        {
            var l_ret = new List<List<Object>>();

            String tablename = pdoc.SelectNodes("//root/select/table_name")[0].InnerText;
            XmlNodeList clist = pdoc.SelectNodes("//root/select/col");
            XmlNodeList join_tablelist = pdoc.SelectNodes("//root/join/table_name");
            XmlNodeList join_onlist = pdoc.SelectNodes("//root/join/on");
            XmlNodeList join_typelist = pdoc.SelectNodes("//root/join/type");
            XmlNodeList whereexpression_list = pdoc.SelectNodes("//root/where/expression");
            XmlNodeList whereewithindex_list = pdoc.SelectNodes("//root/where/withindex");
            XmlNodeList orderby_list = pdoc.SelectNodes("//root/orderby");
            XmlNodeList orderbytype_list = pdoc.SelectNodes("//root/orderbytype");
            XmlNodeList groupby_list = pdoc.SelectNodes("//root/groupby");
            XmlNodeList having_list = pdoc.SelectNodes("//root/having/expression");
            XmlNodeList limit_list = pdoc.SelectNodes("//root/limit");

            if (clist.Count == 0)
                return set_select_error("Select command must contain least one column!");
            
            if (whereexpression_list.Count > 2) {
                var err = new List<Object>();
                err.Add("Select command can't contain more than 2 where clauses!");
                l_ret.Add(err);
                return l_ret;
            }
            if (whereexpression_list.Count == 2) {
                if (whereewithindex_list[0].InnerText == "True" && whereewithindex_list[1].InnerText == "True" || whereewithindex_list[0].InnerText == "False" && whereewithindex_list[1].InnerText == "False") {
                    var err = new List<Object>();
                    err.Add("Two where clauses can't be same type!");
                    l_ret.Add(err);
                    return l_ret;
                }
                else if (whereewithindex_list[1].InnerText == "True") {
                    var err = new List<Object>();
                    err.Add("Second where clause can't be its withindex parameter is True!");
                    l_ret.Add(err);
                    return l_ret;
                }
            }
            if (groupby_list.Count > 1)
            {
                var err = new List<Object>();
                err.Add("Select command can't contain more than one groupby clause!");
                l_ret.Add(err);
                return l_ret;
            }
            if (having_list.Count > 1)
            {
                var err = new List<Object>();
                err.Add("Select command can't contain more than one having clause!");
                l_ret.Add(err);
                return l_ret;
            }
            if (limit_list.Count > 1)
            {
                var err = new List<Object>();
                err.Add("Select command can't contain more than one limit clause!");
                l_ret.Add(err);
                return l_ret;
            }


            var list_select_columns = new List<select_column_struct>();


            for (var i = 0; i < clist.Count; ++i)
            {
                var select_column = new select_column_struct();

                bool space = clist[i].InnerText.Contains(" ");
                if (space)
                {
                    var p = clist[i].InnerText.Split(' ');
                    if (p.Length != 3)
                        return set_select_error((i + 1) + ". column is bad format! Code:1");
                    if (p[1].ToLower() != "as")
                        return set_select_error((i + 1) + ". column is bad format! Code:2");
                    if (p[0] == "" || p[2] == "")
                        return set_select_error((i + 1) + ". column is bad format! Code:3");
                    String fourchars = p[0].ToLower().Substring(0, 4);
                    String sixchars = p[0].ToLower().Substring(0, 6);

                    if (fourchars != "sum(" && fourchars != "avg(" && fourchars != "max(" && fourchars != "min(" && sixchars != "count(")
                        return set_select_error((i + 1) + ". column is containing invalid function!");
                    String name;
                    if (sixchars != "count(")
                        name = p[0].Substring(4);
                    else
                        name = p[0].Substring(6);

                    if (name.Substring(name.Length-1) != ")")
                        return set_select_error((i + 1) + ". column isn't containing closing parenthesis!");
                    name = name.Substring(0, name.Length - 1);
                    select_column.name = name;
                    select_column.function = sixchars == "count(" ? "count" : fourchars.Substring(0, 3);
                    select_column.alias = p[2];
                }
                else
                    select_column.name = clist[i].InnerText;

                list_select_columns.Add(select_column);
            }

            for (var i = 0; i < list_select_columns.Count; ++i) {
                if (list_select_columns[i].function == "count" && list_select_columns.Count - 1 != i)
                    return set_select_error("count() function must be at the end of select columns list!");
            }

            var list_tables = new List<mytable>();

            var firsttable = gettable(db, tablename);

            if (firsttable == null) return set_select_error(tablename +" doesn't exist!");

            list_tables.Add(firsttable);

            for (var i = 0; i < join_tablelist.Count; ++i) {
                var curr_table = gettable(db, join_tablelist[i].InnerText);
                if (curr_table == null) return set_select_error(join_tablelist[i].InnerText + " doesn't exist!");
                list_tables.Add(curr_table);
            }

            var list_spreaded_select_columns = new List<select_column_struct>();
            for (var i = 0; i < list_select_columns.Count; ++i)
            {
                if (list_select_columns[i].name == "*" && list_select_columns[i].function == null)
                {
                    for (var k = 0; k < list_tables.Count; ++k) {
                        for (var j = 0; j < list_tables[k].columns_info.Count; ++j) {
                            select_column_struct spreaded_column = new select_column_struct();
                            spreaded_column.ai = list_tables[k].columns_info[j].ai;
                            spreaded_column.name = list_tables[k].columns_info[j].name;
                            spreaded_column.type = list_tables[k].columns_info[j].type;
                            spreaded_column.table = list_tables[k];
                            spreaded_column.ci = list_tables[k].columns_info[j].ci;

                            var curr_indexes = getindexes(db, list_tables[k].table_name);
                            for (var g = 0; g < curr_indexes.Count; ++g)
                                for (var s = 0; s < curr_indexes[g].sub_indexes.Count; ++s )
                                    if (spreaded_column.name == curr_indexes[g].sub_indexes[s].column_name) {
                                        spreaded_column.index = curr_indexes[g].sub_indexes[s];
                                        goto CIK;
                                    }
                        CIK:
                            list_spreaded_select_columns.Add(spreaded_column);
                        }
                    }
                }
                else
                {
                    var founded = false;

                    for (var k = 0; k < list_tables.Count; ++k)
                    {
                        for (var j = 0; j < list_tables[k].columns_info.Count; ++j)
                        {
                            if (list_select_columns[i].name == list_tables[k].columns_info[j].name) {
                                founded = true;
                                
                                var spreaded_column = new select_column_struct();
                                spreaded_column.ai = list_tables[k].columns_info[j].ai;
                                spreaded_column.name = list_tables[k].columns_info[j].name;
                                spreaded_column.type = list_tables[k].columns_info[j].type;
                                spreaded_column.table = list_tables[k];
                                spreaded_column.alias = list_select_columns[i].alias;
                                spreaded_column.function = list_select_columns[i].function;
                                spreaded_column.ci = list_tables[k].columns_info[j].ci;

                                var curr_indexes = getindexes(db, list_tables[k].table_name);
                                for (var g = 0; g < curr_indexes.Count; ++g)
                                    for (var s = 0; s < curr_indexes[g].sub_indexes.Count; ++s)
                                        if (spreaded_column.name == curr_indexes[g].sub_indexes[s].column_name)
                                        {
                                            spreaded_column.index = curr_indexes[g].sub_indexes[s];
                                            goto CIK2;
                                        }
                            CIK2:
                                list_spreaded_select_columns.Add(spreaded_column);
                            }
                        }
                    }

                    if (!founded && list_select_columns[i].function == null) return set_select_error("The column named ´" + list_select_columns[i].name + "´ was not founded in related table(s)!");
                }
            }

            if (list_select_columns[list_select_columns.Count-1].name == "*" && list_select_columns[list_select_columns.Count-1].function == "count") 
            {
                list_spreaded_select_columns.Add(list_select_columns[list_select_columns.Count - 1]);
            }

            // kolonlar duplicate mi?
            for (var i = 0; i < list_spreaded_select_columns.Count; ++i)
            {
                String curr_name = list_spreaded_select_columns[i].alias;
                if (curr_name == null || curr_name == "") curr_name = list_spreaded_select_columns[i].name;

                for (var k = i + 1; k < list_spreaded_select_columns.Count; ++k)
                {
                    String curr_name2 = list_spreaded_select_columns[k].alias;
                    if (curr_name2 == null || curr_name2 == "") curr_name2 = list_spreaded_select_columns[k].name;

                    if (curr_name == curr_name2)
                        return set_select_error("The column named `" + curr_name + "` is ambigious!");
                }
            }
         
            // kolonlar birden fazla tablda tanımlı mı?
            for (var i = 0; i < list_spreaded_select_columns.Count; ++i)
            {
                String curr_name = list_spreaded_select_columns[i].name;
                int defined_count = 0;

                for (var k = 0; k < list_tables.Count; ++k)
                    for (var j = 0; j < list_tables[k].columns_info.Count; ++j) {
                        if (curr_name == list_tables[k].columns_info[j].name) {
                            if (++defined_count > 1)
                                return set_select_error("The column named `" + curr_name + "` is defined in more than one table defined!");
                        }
                    }
            }


            // ilk where deyimi indexle mi alakalı?
            var kv_index_queried_columns = new List<select_column_struct>();

            if (whereexpression_list.Count > 0 && whereewithindex_list[0].InnerText == "True")
            {
                String indexed_where_exp = whereexpression_list[0].InnerText;
                if (indexed_where_exp == "") return set_select_error("First where clause is empty!");

                var testor = Regex.Split(indexed_where_exp, "\\|\\|(?=(?:[^']*'[^']*')*[^']*$)");
                if (testor.Length > 1) return set_select_error("|| operator is unsopperted in indexed where() function!");

                var parts = Regex.Split(indexed_where_exp, "&&(?=(?:[^']*'[^']*')*[^']*$)");

                for (var i = 0; i < parts.Count(); ++i)
                {
                    var m = parts[i];
                    var curr_op = "";
                    var pp = Regex.Split(m, " not like (?=(?:[^']*'[^']*')*[^']*$)");

                    if (pp.Length < 2)
                    {
                        pp = Regex.Split(m, "==(?=(?:[^']*'[^']*')*[^']*$)");
                        if (pp.Length < 2)
                        {
                            pp = Regex.Split(m, " like (?=(?:[^']*'[^']*')*[^']*$)");
                            if (pp.Length < 2)
                            {
                                pp = Regex.Split(m, "!=(?=(?:[^']*'[^']*')*[^']*$)");
                                if (pp.Length < 2)
                                {
                                    pp = Regex.Split(m, ">=(?=(?:[^']*'[^']*')*[^']*$)");
                                    if (pp.Length < 2)
                                    {
                                        pp = Regex.Split(m, "<=(?=(?:[^']*'[^']*')*[^']*$)");
                                        if (pp.Length < 2)
                                        {
                                            pp = Regex.Split(m, ">(?=(?:[^']*'[^']*')*[^']*$)");
                                            if (pp.Length < 2)
                                            {
                                                pp = Regex.Split(m, "<(?=(?:[^']*'[^']*')*[^']*$)");
                                                if (pp.Length < 2)
                                                    return set_select_error("There is invalid operator at first where expression!");
                                                else
                                                    curr_op = "<";
                                            }
                                            else
                                                curr_op = ">";
                                        }
                                        else
                                            curr_op = "<=";
                                    }
                                    else
                                        curr_op = ">=";
                                }
                                else
                                    curr_op = "!=";
                            }
                            else
                                curr_op = "like";
                        }
                        else
                            curr_op = "=";
                    }
                    else
                        curr_op = "not like";

                    String curr_column = pp[0].Trim();
                    String curr_value = pp[1].Trim();

                    var founded = false;

                    for (var k = 0; k < list_tables.Count; ++k)
                    {
                        for (var j = 0; j < list_tables[k].columns_info.Count; ++j)
                        {
                            if (curr_column == list_tables[k].columns_info[j].name)
                            {
                                founded = true;
                                select_column_struct column = new select_column_struct();
                                column.ai = list_tables[k].columns_info[j].ai;
                                column.ci = list_tables[k].columns_info[j].ci;
                                column.name = list_tables[k].columns_info[j].name;
                                column.type = list_tables[k].columns_info[j].type;
                                column.table = list_tables[k];
                                column.value = curr_value;
                                column.op = curr_op;
                                column.index = null;
                                var curr_indexes = getindexes(db, list_tables[k].table_name);
                                for (var g = 0; g < curr_indexes.Count; ++g)
                                    for (var s = 0; s < curr_indexes[g].sub_indexes.Count; ++s)
                                        if (column.name == curr_indexes[g].sub_indexes[s].column_name)
                                        {
                                            column.index = curr_indexes[g].sub_indexes[s];
                                            goto CIK4;
                                        }
                            CIK4:
                                kv_index_queried_columns.Add(column);
                            }
                        }
                    }

                    if (!founded) set_select_error("The column named ´" + curr_column + "´ was not founded in related table(s)! Code:2");
                }
            }

            //dosya gostericisi pozisyonlarını index dosyalarından sorgula
            var list_cursor_positions = new List<List<Int64>>();

            for (var i = 0; i < kv_index_queried_columns.Count; ++i) {
                if (kv_index_queried_columns[i].index == null)
                    return set_select_error(kv_index_queried_columns[i].name +" has no index!");

                var cursor_positions = new List<Int64>();

                

                String index_key = kv_index_queried_columns[i].value;
                String op = kv_index_queried_columns[i].op;
                var ci = kv_index_queried_columns[i].ci;

                var sql = "";
                if (index_key == "NULL")
                {
                    if (op == "=")
                        sql = "SELECT * FROM keyvalue WHERE [key] ISNULL";
                    else if (op == "!=")
                        sql = "SELECT * FROM keyvalue WHERE NOT ([key] ISNULL)";
                    else
                        sql = "SELECT * FROM keyvalue WHERE [key] " + op + " NULL";
                }
                else if (ci == true && (op == "like" || op == "not like"))
                    sql = "SELECT * FROM keyvalue WHERE TOUPPER([key]) " + op + " TOUPPER(" + index_key +")";
                else
                    sql = "SELECT * FROM keyvalue WHERE [key] " + op + " " + index_key;


                var cmd = new SQLiteCommand(sql, kv_index_queried_columns[i].index.index_file);
                SQLiteDataReader r;
                try
                {
                    r = cmd.ExecuteReader();
                }
                catch (Exception e)
                {
                    cmd.Dispose();
                    
                    return set_select_error(e.Message);
                } 

                while (r.Read())
                {
                    cursor_positions.Add((long) r["value"]);   
                }

                r.Close();
                cmd.Dispose();
                
                

                list_cursor_positions.Add(cursor_positions);
            }

            //gösterici pozisyonlarının kesişimlerini al
            var list_cursor_positions_son = new List<List<Int64>>();

            for (var i = 0; i < kv_index_queried_columns.Count; ++i)
            {
                var curr_cursor_positions = list_cursor_positions[i];
                for (var k = 0; k < kv_index_queried_columns.Count; ++k) {
                    if (i == k)
                        continue;
                    if (kv_index_queried_columns[i].table.table_name == kv_index_queried_columns[k].table.table_name) {
                        curr_cursor_positions = curr_cursor_positions.Intersect(list_cursor_positions[k]).ToList();
                    }
                }
                list_cursor_positions_son.Add(curr_cursor_positions);
            }

            var list_cursor_positions_son2 = list_cursor_positions_son;
            
            //tablolar için tanımlı olmayanlara pozisyon değeri olarak -1 ata
            var list_cursor_positions_son3 = new List<List<Int64>>();

            for (var i = 0; i < list_tables.Count; ++i) {
                var poses = new List<Int64>();
                var found = false;
                for (var k = 0; k < kv_index_queried_columns.Count; ++k)
                {
                    if (list_tables[i].table_name == kv_index_queried_columns[k].table.table_name)
                    {
                        found = true;
                        for (var j = 0; j < list_cursor_positions_son2[k].Count; ++j)
                            poses.Add(list_cursor_positions_son2[k][j]);
                    }
                }

                //if (!found) poses.Add(-1);
                list_cursor_positions_son3.Add(poses);
            }

            for (var i = 0; i < list_cursor_positions_son3.Count; ++i) {
                if (list_cursor_positions_son3[i].Count == 0)
                {
                    var curr_tablename = list_tables[i].table_name;
                    var found = false;

                    for (var k = 0; k < kv_index_queried_columns.Count; ++k )
                        if (curr_tablename == kv_index_queried_columns[k].table.table_name) {
                            found = true;
                            break;
                        }

                    if (!found) list_cursor_positions_son3[i].Add(-1);
                }
            }

            for (var i = 0; i < list_cursor_positions_son3.Count; ++i)
                list_cursor_positions_son3[i] = list_cursor_positions_son3[i].Distinct().ToList();
            
            var whole_records = new List<List<List<Object>>>();
            
            // dosya işlemleri
            int size = 0;
            List<Stream> fps = new List<Stream>();
            for (var i = 0; i < list_tables.Count; ++i) {
                
                Stream fp = File.Open(list_tables[i].data_file_path, FileMode.Open, FileAccess.Read, FileShare.Read);
                fps.Add(fp);
            }

            for (var i = 0; i < list_cursor_positions_son3.Count; ++i) {
                var curr_table_records = new List<List<Object>>();
                if (list_cursor_positions_son3[i].Count == 0)
                    continue;

                if (list_cursor_positions_son3[i][0] == -1) // tüm kayıtları getir
                {
                    while (true)
                    {
                        var curr_record = new List<Object>();
                        var deleted_flag = new byte[1];
                        if (fps[i].Read(deleted_flag, 0, 1) == 0)
                            break;

                        if (deleted_flag[0] == 1) 
                        {
                            for (var k = 0; k < list_tables[i].columns_info.Count; ++k)
                            {
                                var null_flag = new byte[1];
                                fps[i].Read(null_flag, 0, 1);
                                if (null_flag[0] == 1)
                                {
                                    fps[i].Seek(1, SeekOrigin.Current);
                                    continue;
                                }

                                switch (list_tables[i].columns_info[k].type)
                                {
                                    case "tinyint": size = sizeof(sbyte); break;
                                    case "smallint": size = sizeof(Int16); break;
                                    case "int": size = sizeof(Int32); break;
                                    case "bigint": size = sizeof(Int64); break;
                                    case "text":
                                        var bytesofsize = new byte[sizeof(int)];
                                        fps[i].Read(bytesofsize, 0, sizeof(int));
                                        size = BitConverter.ToInt32(bytesofsize, 0);
                                        break;
                                    case "double": size = sizeof(double); break;
                                }
                                fps[i].Seek(size, SeekOrigin.Current);
                            }
                            continue;
                        }

                        var curr_column_value = new Object();
                        // deleted flag = 0, verileri çek
                        
                        for (var j = 0; j < list_tables[i].columns_info.Count; ++j)
                        {
                            var null_flag = new byte[1];
                            fps[i].Read(null_flag, 0, 1);
                            if (null_flag[0] == 1)
                            {
                                fps[i].Seek(1, SeekOrigin.Current);
                                curr_record.Add(null);
                                continue;
                            }

                            byte[] curr_column_bytes;

                            switch (list_tables[i].columns_info[j].type)
                            {
                                case "tinyint": 
                                    size = sizeof(sbyte);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = curr_column_bytes[0];
                                    break;
                                case "smallint": 
                                    size = sizeof(Int16); 
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt16(curr_column_bytes, 0);
                                    break;
                                case "int": size = 
                                    sizeof(Int32); 
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt32(curr_column_bytes, 0);
                                    break;
                                case "bigint": 
                                    size = sizeof(Int64); 
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt64(curr_column_bytes, 0);
                                    break;
                                case "text":
                                    var bytesofsize = new byte[sizeof(int)];
                                    fps[i].Read(bytesofsize, 0, sizeof(int));
                                    size = BitConverter.ToInt32(bytesofsize, 0);

                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);

                                    curr_column_value = Encoding.Unicode.GetString(curr_column_bytes, 0, size);
                                    break;
                                case "double": 
                                    size = sizeof(double);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToDouble(curr_column_bytes, 0);
                                    break;
                                        
                            }

                            for (var k = 0; k < list_spreaded_select_columns.Count; ++k)
                            {
                                if (list_tables[i].columns_info[j].name == list_spreaded_select_columns[k].name)
                                {
                                    curr_record.Add(curr_column_value);
                                }
                            }
                        }

                        curr_table_records.Add(curr_record);
                    }
                    
                }
                else // indexten gelen kayıtları getir
                {
                    for (var g = 0; g < list_cursor_positions_son3[i].Count; ++g)
                    {
                        var curr_record = new List<Object>();
                        Int64 curr_pos = list_cursor_positions_son3[i][g];

                        fps[i].Seek(curr_pos, SeekOrigin.Begin);

                        var deleted_flag = new byte[1];
                        fps[i].Read(deleted_flag, 0, 1);

                        if (deleted_flag[0] == 1)
                            continue;

                        var curr_column_value = new Object();
                        // deleted flag = 0, verileri çek

                        for (var j = 0; j < list_tables[i].columns_info.Count; ++j)
                        {
                            var null_flag = new byte[1];
                            fps[i].Read(null_flag, 0, 1);
                            if (null_flag[0] == 1)
                            {
                                fps[i].Seek(1, SeekOrigin.Current);
                                curr_record.Add(null);
                                continue;
                            }

                            byte[] curr_column_bytes;

                            switch (list_tables[i].columns_info[j].type)
                            {
                                case "tinyint":
                                    size = sizeof(sbyte);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = curr_column_bytes[0];
                                    break;
                                case "smallint":
                                    size = sizeof(Int16);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt16(curr_column_bytes, 0);
                                    break;
                                case "int": size =
                                    sizeof(Int32);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt32(curr_column_bytes, 0);
                                    break;
                                case "bigint":
                                    size = sizeof(Int64);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt64(curr_column_bytes, 0);
                                    break;
                                case "text":
                                    var bytesofsize = new byte[sizeof(int)];
                                    fps[i].Read(bytesofsize, 0, sizeof(int));
                                    size = BitConverter.ToInt32(bytesofsize, 0);

                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);

                                    curr_column_value = Encoding.Unicode.GetString(curr_column_bytes, 0, size);
                                    break;
                                case "double":
                                    size = sizeof(double);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToDouble(curr_column_bytes, 0);
                                    break;

                            }

                            for (var k = 0; k < list_spreaded_select_columns.Count; ++k)
                            {
                                if (list_tables[i].columns_info[j].name == list_spreaded_select_columns[k].name)
                                {
                                    curr_record.Add(curr_column_value);
                                }
                            }
                        }
                            

                        curr_table_records.Add(curr_record);
                    }
                }

                whole_records.Add(curr_table_records);
            }

            for (var i = 0; i < list_tables.Count; ++i)
            {
                fps[i].Close();
                
            }

            //linqtosql sorguları
            var on_exp_list = new List<String>();
            for (var i = 0; i < join_onlist.Count; ++i) {
                var curr_on = join_onlist[i].InnerText;
                var parts = Regex.Split(curr_on, "==(?=(?:[^']*'[^']*')*[^']*$)");
                if (parts.Length < 2) return set_select_error((i+1)+". on expression must contain == operator!");
                var column1 = parts[0].Trim();
                var column2 = parts[1].Trim();

                var order1 = get_column_order(column1, ref list_spreaded_select_columns);
                var order2 = get_column_order(column2, ref list_spreaded_select_columns);

                var type = join_typelist[i].InnerText;

                if (type == "inner") {
                    column1 = order1.table_name + "_sl[" + order1.indis + "]";
                    column2 = order2.table_name + "_sl[" + order2.indis + "]";
                }
                else
                {
                    column1 = order1.table_name + "_sl[" + order1.indis + "]";
                    column2 = "lr"+ (i+1) +"_sl[" + order2.indis + "]";
                    if (list_tables[i + 1].table_name != order2.table_name)
                        return set_select_error((i + 1) + ". `on` parameter's right table's column name is invalid!");
                }
                if (order1.ci == true && order2.ci == true)
                {
                    column1 += ".ToString().ToLower()";
                    column2 += ".ToString().ToLower()";
                }
                on_exp_list.Add(column1 +" equals "+ column2);
            }

            String where_exp_without_index = "";

            if (whereexpression_list.Count == 1 && whereewithindex_list[0].InnerText == "False")
            {
                where_exp_without_index = whereexpression_list[0].InnerText;
            }
            else if (whereexpression_list.Count == 2 && whereewithindex_list[1].InnerText == "False")
            {
                where_exp_without_index = whereexpression_list[1].InnerText;
            }

            if (list_tables.Count > 1 || where_exp_without_index != "" || groupby_list.Count > 0 || orderby_list.Count > 0)
            {

                var sql = "(\nfrom " + list_tables[0].table_name + "_sl in tables[0]\n";
                for (var i = 1; i < list_tables.Count; ++i)
                {
                    if (join_typelist[i-1].InnerText == "inner")
                        sql += "join " + list_tables[i].table_name + "_sl in tables[" + i + "] on " + on_exp_list[i - 1] + "\n";
                    else
                    {
                        sql += "join lr" + i + "_sl in tables[" + i + "] on " + on_exp_list[i - 1] + " into lrs" + i + "\n";
                        sql += "from " + list_tables[i].table_name + "_sl in lrs" + i + ".DefaultIfEmpty()\n";
                    }
                }
                // where exp çözümle

                if (where_exp_without_index != "")
                {
                    where_exp_without_index = Regex.Replace(where_exp_without_index, "&&(?=(?:[^']*'[^']*')*[^']*$)", "&");
                    where_exp_without_index = Regex.Replace(where_exp_without_index, "\\|\\|(?=(?:[^']*'[^']*')*[^']*$)", "|");

                    var mc = Regex.Split(where_exp_without_index, "[&|](?=(?:[^']*'[^']*')*[^']*$)");

                    var parts = new List<String>();

                    foreach (String m in mc) {
                        var sm = Regex.Replace(m, "\\((?=(?:[^']*'[^']*')*[^']*$)", "");
                        sm = Regex.Replace(sm, "\\)(?=(?:[^']*'[^']*')*[^']*$)", "");
                        sm = sm.Trim();
                        parts.Add(sm);        
                    }

                    parts = (from s in parts
                             orderby s.Length descending
                             select s).ToList();
                    var parts2 = new List<String>();
                    
                    for (var i = 0; i < parts.Count; ++i)
                    {
                        var oper = "not like";
                        var p = Regex.Split(parts[i], " not like (?=(?:[^']*'[^']*')*[^']*$)");
                        if (p.Length == 1) 
                        { 
                            p = Regex.Split(parts[i], " like (?=(?:[^']*'[^']*')*[^']*$)");
                            if (p.Length > 1) oper = "like";
                        }
                        if (p.Length > 1) {
                            p[0] = p[0].Trim();
                            p[1] = p[1].Trim();

                            if (p[0] == "" || p[1] == "")
                                return set_select_error("Statement was bad format!");

                            var p0isvalue = false;
                            var p1isvalue = false;

                            if (p[0].Substring(0, 1) == "'")
                            {
                                p0isvalue = true;
                                p[0] = p[0].Substring(1);
                                if (p[0].Length > 0 && p[0].Substring(p[0].Length - 1, 1) == "'")
                                    p[0] = p[0].Substring(0, p[0].Length - 1);
                            }
                            else if (p[0] == "NULL")
                                p[0] = null;

                            if (p[1].Substring(0, 1) == "'")
                            {
                                p1isvalue = true;
                                p[1] = p[1].Substring(1);
                                if (p[1].Length > 0 && p[1].Substring(p[1].Length - 1, 1) == "'")
                                    p[1] = p[1].Substring(0, p[1].Length - 1);
                            }
                            else if (p[1] == "NULL")
                                p[1] = null;

             
                            column_order_struct o = get_column_order(p[0], ref list_spreaded_select_columns);
                            column_order_struct o2 = get_column_order(p[1], ref list_spreaded_select_columns);

                            var cond = oper == "like" ? "true" : "false";

                            var ekcik = "";
                            var ekciu = "";
                            var ekcik0 = "";
                            var ekciu0 = "";
                            var ekcik1 = "";
                            var ekciu1 = "";

                            if ((o.ci == true && o2.ci == true) || (o.ci == true && p1isvalue) || (p0isvalue && o2.ci == true))
                            {
                                ekcik = ".ToLower()";
                                ekciu = ".ToString().ToLower()";
                                ekcik0 = ".ToLower()";
                                ekciu0 = ".ToString().ToLower()";
                                ekcik1 = ".ToLower()";
                                ekciu1 = ".ToString().ToLower()";

                                if (p[0] == null)
                                {
                                    ekcik0 = "";
                                    ekciu0 = "";
                                }
                                if (p[1] == null)
                                {
                                    ekcik1 = "";
                                    ekciu1 = "";
                                }
                            }

                            String part = "";
                            if (o.table_name != null && o2.table_name != null)
                                part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "] == null) ? \"\" : " + o2.table_name + "_sl[" + o2.indis + "].ToString()"+ ekcik +"), ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "] == null) ? \"\" : " + o.table_name + "_sl[" + o.indis + "].ToString()"+ ekcik +")) == "+ cond;
                            else if (o.table_name == null && o2.table_name == null)
                                part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(\"" + p[1] + "\"" + ekciu1 + ", ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(\"" + p[0] + "\"" + ekciu0 + ")) == " + cond;
                            else if (o.table_name == null)
                                part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "] == null) ? \"\" : " + o2.table_name + "_sl[" + o2.indis + "].ToString()" + ekcik + "), ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(\"" + p[0] + "\"" + ekciu0 + ") == " + cond;
                            else
                                part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(\"" + p[1] + "\"" + ekciu1 + ", ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "] == null) ? \"\" : " + o.table_name + "_sl[" + o.indis + "].ToString()" + ekcik + ")) == " + cond;
                           
                            parts2.Add(part);
                        }
                        else {
                            String op = "";

                            p = Regex.Split(parts[i], "==(?=(?:[^']*'[^']*')*[^']*$)");
                            if (p.Length == 2)
                                op = "==";
                            else {
                                p = Regex.Split(parts[i], "!=(?=(?:[^']*'[^']*')*[^']*$)");
                                if (p.Length == 2)
                                    op = "!=";
                                else
                                {
                                    p = Regex.Split(parts[i], ">=(?=(?:[^']*'[^']*')*[^']*$)");
                                    if (p.Length == 2)
                                        op = ">=";
                                    else
                                    {
                                        p = Regex.Split(parts[i], "<=(?=(?:[^']*'[^']*')*[^']*$)");
                                        if (p.Length == 2)
                                            op = "<=";
                                        else
                                        {
                                            p = Regex.Split(parts[i], ">(?=(?:[^']*'[^']*')*[^']*$)");
                                            if (p.Length == 2)
                                                op = ">";
                                            else
                                            {
                                                p = Regex.Split(parts[i], "<(?=(?:[^']*'[^']*')*[^']*$)");
                                                if (p.Length == 2)
                                                    op = "<";
                                                else
                                                {
                                                    return set_select_error("Invalid operator(s) in where clause!");
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            p[0] = p[0].Trim();
                            p[1] = p[1].Trim();

                            if (p[0] == "" || p[1] == "")
                                return set_select_error("Statement was bad format!");


                            var p0isvalue = false;
                            var p1isvalue = false;

                            if (p[0].Substring(0, 1) == "'")
                            {
                                p0isvalue = true;
                                p[0] = p[0].Substring(1);
                                if (p[0].Length > 0 && p[0].Substring(p[0].Length - 1, 1) == "'")
                                    p[0] = p[0].Substring(0, p[0].Length - 1);
                            }
                            else if (p[0] == "NULL")
                                p[0] = null;

                            if (p[1].Substring(0, 1) == "'")
                            {
                                p1isvalue = true;
                                p[1] = p[1].Substring(1);
                                p[1] = p[1].Substring(0, p[1].Length - 1);
                            }
                            else if (p[1] == "NULL")
                                p[1] = null;

                            column_order_struct o = get_column_order(p[0], ref list_spreaded_select_columns);
                            column_order_struct o2 = get_column_order(p[1], ref list_spreaded_select_columns);

                            var ekcik = "";
                            var ekciu = "";
                            var ekcik0 = "";
                            var ekciu0 = "";
                            var ekcik1 = "";
                            var ekciu1 = "";

                            if ((o.ci == true && o2.ci == true) || (o.ci == true && p1isvalue) || (p0isvalue && o2.ci == true))
                            {
                                ekcik = ".ToLower()";
                                ekciu = ".ToString().ToLower()";
                                ekcik0 = ".ToLower()";
                                ekciu0 = ".ToString().ToLower()";
                                ekcik1 = ".ToLower()";
                                ekciu1 = ".ToString().ToLower()";

                                if (p[0] == null)
                                {
                                    ekcik0 = "";
                                    ekciu0 = "";
                                }
                                if (p[1] == null)
                                {
                                    ekcik1 = "";
                                    ekciu1 = "";
                                }
                            }

                            String part = "";
                            if (o.table_name != null && o2.table_name != null)
                            {
                                var type1 = list_spreaded_select_columns[o.indis2].type;
                                var type2 = list_spreaded_select_columns[o2.indis2].type;
                                if (type1 == "text" || type2 == "text")
                                    part = "string.Compare(((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "]==null) ? null : " + o.table_name + "_sl[" + o.indis + "].ToString()"+ ekcik +"), ((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "]==null) ? null : " + o2.table_name + "_sl[" + o2.indis + "].ToString()" + ekcik + ")) " + op + " 0";
                                else {
                                    part = "((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "]==null) ? 0 : double.Parse(" + o.table_name + "_sl[" + o.indis + "].ToString())) ";
                                    part += op +" ";
                                    part += "((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "]==null) ? 0 : double.Parse(" + o2.table_name + "_sl[" + o2.indis + "].ToString()))";
                                }
                            }
                            else if (o.table_name == null && o2.table_name == null)
                            {
                                double res = 0;
                                Int64 res2;

                                var type1 = double.TryParse(p[0], out res) || Int64.TryParse(p[0], out res2) ? "" : "text";
                                var type2 = double.TryParse(p[1], out res) || Int64.TryParse(p[1], out res2) ? "" : "text";

                                if (type1 == "text" || type2 == "text")
                                    part = "string.Compare(" + (p[0] == null ? "null" : "\"" + p[0] + "\"" + ekciu0 + "") + ", " + (p[1] == null ? "null" : "\"" + p[1] + "\"" + ekciu1 + "") + ") " + op + " 0";
                                else
                                {
                                    part = p[0] + " ";
                                    part += op + " ";
                                    part += p[1];
                                }

                            }
                            else if (o.table_name == null)
                            {
                                double res = 0;
                                Int64 res2;

                                var type1 = double.TryParse(p[0], out res) || Int64.TryParse(p[0], out res2) ? "" : "text";
                                var type2 = list_spreaded_select_columns[o2.indis2].type;

                                if (type1 == "text" || type2 == "text")
                                    part = "string.Compare(" + (p[0] == null ? "null" : "\"" + p[0] + "\"" + ekciu0 + "") + ", ((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "]==null) ? null : " + o2.table_name + "_sl[" + o2.indis + "].ToString()" + ekcik + ")) " + op + " 0";
                                else
                                {
                                    part = p[0] + " ";
                                    part += op + " ";
                                    part += "((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "]==null) ? 0 : double.Parse(" + o2.table_name + "_sl[" + o2.indis + "].ToString()))";
                                }

                            }
                            else if (o2.table_name == null)
                            {
                                double res = 0;
                                Int64 res2 = 0;

                                var type1 = list_spreaded_select_columns[o.indis2].type;
                                var type2 = double.TryParse(p[1], out res) || Int64.TryParse(p[1], out res2) ? "" : "text";

                                if (type1 == "text" || type2 == "text")
                                    part = "string.Compare(((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "]==null) ? null : " + o.table_name + "_sl[" + o.indis + "].ToString()" + ekcik + "), " + (p[1] == null ? "null" : "\"" + p[1] + "\"" + ekciu1 + "") + ") " + op + " 0";
                                else
                                {
                                    part += "((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "]==null) ? 0 : double.Parse(" + o.table_name + "_sl[" + o.indis + "].ToString())) ";
                                    part += op + " ";
                                    part += p[1];
                                }

                            }
                            
                            parts2.Add(part);
                        }
                    }

                    where_exp_without_index = Regex.Replace(where_exp_without_index, "&(?=(?:[^']*'[^']*')*[^']*$)", "&&");
                    where_exp_without_index = Regex.Replace(where_exp_without_index, "\\|(?=(?:[^']*'[^']*')*[^']*$)", "||");

                    for (var i = 0; i < parts.Count; ++i)
                        where_exp_without_index = where_exp_without_index.Replace(parts[i], parts2[i]);

                    sql += "where " + where_exp_without_index + "\n";
                }

                var new_str = "";
                for (var i = 0; i < list_spreaded_select_columns.Count; ++i) {
                    if (list_spreaded_select_columns[i].name == "*")
                        new_str += "null,";
                    else 
                    {
                        var o = get_column_order(list_spreaded_select_columns[i].name, ref list_spreaded_select_columns);
                        new_str += "(" + o.table_name + "_sl==null?null:" + o.table_name + "_sl["+ o.indis +"]),";
                    }
                }



                new_str = new_str.Substring(0, new_str.Length-1);
                sql += "select new List<Object> {"+ new_str +"}\n)";

                if (groupby_list.Count > 0) {
                    var gb_columns = groupby_list[0].InnerText.Split(',');
                    for (var i = 0; i < gb_columns.Count(); ++i)
                    {
                        gb_columns[i] = gb_columns[i].Trim();
                        var order = get_column_order(gb_columns[i], ref list_spreaded_select_columns);
                        if (order.table_name == null) return set_select_error("Column named ´" + gb_columns[i] + "´ at groupby clause could not find in select column list!");
                    }

                    
                    for (var i = 0; i < list_spreaded_select_columns.Count; ++i) {
                        if (list_spreaded_select_columns[i].name == "*" || list_spreaded_select_columns[i].function != null)
                            continue;
                        var found = false;
                        for (var k = 0; k < gb_columns.Count(); ++k) {
                            if (gb_columns[k] == list_spreaded_select_columns[i].name) {
                                found = true;
                                break;
                            }
                        }
                        if (!found) return set_select_error("Column named ´"+ list_spreaded_select_columns[i].name +"´ could not find in groupby column list!");
                    }

                    var new_grp = "";

                    for (var i = 0; i < gb_columns.Count(); ++i) {
                        new_grp += gb_columns[i] + "=";
                        var order = get_column_order(gb_columns[i], ref list_spreaded_select_columns);
                   
                        new_grp += "g["+ order.indis2 +"],";
                    }
                    new_grp = new_grp.Substring(0, new_grp.Length - 1);

                    var new_select = "";
                    for (var i = 0; i < gb_columns.Count(); ++i)
                    {
                        new_select += "s.Key."+ gb_columns[i] +",";
                    }

                    int indis = 0;
                    while (indis < list_spreaded_select_columns.Count && list_spreaded_select_columns[indis].function == null)
                        ++indis;

                    while (indis < list_spreaded_select_columns.Count)
                    {
                        var function = list_spreaded_select_columns[indis].function;
                        if (function == null) return set_select_error("Select list is bad format!");
                        if (function == "min") function = "Min";
                        else if (function == "max") function = "Max";
                        else if (function == "sum") function = "Sum";
                        else if (function == "avg") function = "Average";
                        else function = "Count";

                        if (function == "Count")
                        {
                            new_select += "s.Count(),";
                        }
                        else {
                            new_select += "s." + function + "(a => double.Parse( (a["+indis+"] == null ? 0 : a["+indis+"]).ToString()) ),";
                        }

                        ++indis;
                    }

                    new_select = new_select.Substring(0, new_select.Length - 1);

                    sql += ".GroupBy(g => new { " + new_grp + " })";
                    sql += ".Select(s => new List<Object> {"+ new_select +"})";

                    //-------having var mı?
                    if (having_list.Count > 0)
                    {
                        var havingstr = having_list[0].InnerText;

                        havingstr = Regex.Replace(havingstr, "&&(?=(?:[^']*'[^']*')*[^']*$)", "&");
                        havingstr = Regex.Replace(havingstr, "\\|\\|(?=(?:[^']*'[^']*')*[^']*$)", "|");

                        var mc = Regex.Split(havingstr, "[&|](?=(?:[^']*'[^']*')*[^']*$)");

                        var parts = new List<String>();

                        foreach (String m in mc)
                        {
                            var sm = Regex.Replace(m, "\\((?=(?:[^']*'[^']*')*[^']*$)", "");
                            sm = Regex.Replace(sm, "\\)(?=(?:[^']*'[^']*')*[^']*$)", "");
                            sm = sm.Trim();
                            parts.Add(sm);
                        }

                        parts = (from s in parts
                                 orderby s.Length descending
                                 select s).ToList();
                        var parts2 = new List<String>();

                        for (var i = 0; i < parts.Count; ++i)
                        {
                            var oper = "not like";
                            var p = Regex.Split(parts[i], " not like (?=(?:[^']*'[^']*')*[^']*$)");
                            if (p.Length == 1)
                            {
                                p = Regex.Split(parts[i], " like (?=(?:[^']*'[^']*')*[^']*$)");
                                if (p.Length > 1) oper = "like";
                            }
                            if (p.Length > 1)
                            {
                                p[0] = p[0].Trim();
                                p[1] = p[1].Trim();

                                if (p[0] == "" || p[1] == "")
                                    return set_select_error("Having statement was bad format!");

                                var p0isvalue = false;
                                var p1isvalue = false;

                                if (p[0].Substring(0, 1) == "'")
                                {
                                    p0isvalue = true;
                                    p[0] = p[0].Substring(1);
                                    if (p[0].Length > 0 && p[0].Substring(p[0].Length - 1, 1) == "'")
                                        p[0] = p[0].Substring(0, p[0].Length - 1);
                                }
                                else if (p[0] == "NULL")
                                    p[0] = null;

                                if (p[1].Substring(0, 1) == "'")
                                {
                                    p1isvalue = true;
                                    p[1] = p[1].Substring(1);
                                    if (p[1].Length > 0 && p[1].Substring(p[1].Length - 1, 1) == "'")
                                        p[1] = p[1].Substring(0, p[1].Length - 1);
                                }
                                else if (p[1] == "NULL")
                                    p[1] = null;


                                column_order_struct o = get_column_order(p[0], ref list_spreaded_select_columns);
                                column_order_struct o2 = get_column_order(p[1], ref list_spreaded_select_columns);

                                var cond = oper == "like" ? "true" : "false";

                                var ekcik = "";
                                var ekciu = "";
                                var ekcik0 = "";
                                var ekciu0 = "";
                                var ekcik1 = "";
                                var ekciu1 = "";

                                if ((o.ci == true && o2.ci == true) || (o.ci == true && p1isvalue) || (p0isvalue && o2.ci == true))
                                {
                                    ekcik = ".ToLower()";
                                    ekciu = ".ToString().ToLower()";
                                    ekcik0 = ".ToLower()";
                                    ekciu0 = ".ToString().ToLower()";
                                    ekcik1 = ".ToLower()";
                                    ekciu1 = ".ToString().ToLower()";

                                    if (p[0] == null)
                                    {
                                        ekcik0 = "";
                                        ekciu0 = "";
                                    }
                                    if (p[1] == null)
                                    {
                                        ekcik1 = "";
                                        ekciu1 = "";
                                    }
                                }

                                String part = "";
                                if (o.table_name != null && o2.table_name != null)
                                    part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(hw["+o2.indis2+ "]==null?null:hw[" + o2.indis2 + "]" + ekciu + ", ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(hw[" + o.indis2+ "]==null?null:hw[" + o.indis2 + "]" + ekciu + ") == " + cond;
                                else if (o.table_name == null && o2.table_name == null)
                                    part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(\"" + p[1] + "\"" + ekciu1 + ", ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(\"" + p[0] + "\"" + ekciu0 + ")) == " + cond;
                                else if (o.table_name == null)
                                    part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(hw[" + o2.indis2 + "]==null?null:hw[" + o2.indis2 + "]" + ekciu + ", ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(\"" + p[0] + "\"" + ekciu0 + ") == " + cond;
                                else
                                    part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(\"" + p[1] + "\"" + ekciu1 + ", ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(hw[" + o.indis2 + "]==null?null:hw[" + o.indis2 + "]" + ekciu + ") == " + cond;

                                parts2.Add(part);
                            }
                            else
                            {
                                String op = "";

                                p = Regex.Split(parts[i], "==(?=(?:[^']*'[^']*')*[^']*$)");
                                if (p.Length == 2)
                                    op = "==";
                                else
                                {
                                    p = Regex.Split(parts[i], "!=(?=(?:[^']*'[^']*')*[^']*$)");
                                    if (p.Length == 2)
                                        op = "!=";
                                    else
                                    {
                                        p = Regex.Split(parts[i], ">=(?=(?:[^']*'[^']*')*[^']*$)");
                                        if (p.Length == 2)
                                            op = ">=";
                                        else
                                        {
                                            p = Regex.Split(parts[i], "<=(?=(?:[^']*'[^']*')*[^']*$)");
                                            if (p.Length == 2)
                                                op = "<=";
                                            else
                                            {
                                                p = Regex.Split(parts[i], ">(?=(?:[^']*'[^']*')*[^']*$)");
                                                if (p.Length == 2)
                                                    op = ">";
                                                else
                                                {
                                                    p = Regex.Split(parts[i], "<(?=(?:[^']*'[^']*')*[^']*$)");
                                                    if (p.Length == 2)
                                                        op = "<";
                                                    else
                                                    {
                                                        return set_select_error("Invalid operator(s) in where clause!");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                p[0] = p[0].Trim();
                                p[1] = p[1].Trim();

                                if (p[0] == "" || p[1] == "")
                                    return set_select_error("Having statement was bad format!");


                                var p0isvalue = false;
                                var p1isvalue = false;

                                if (p[0].Substring(0, 1) == "'")
                                {
                                    p0isvalue = true;
                                    p[0] = p[0].Substring(1);
                                    if (p[0].Length > 0 && p[0].Substring(p[0].Length - 1, 1) == "'")
                                        p[0] = p[0].Substring(0, p[0].Length - 1);
                                }
                                else if (p[0] == "NULL")
                                    p[0] = null;

                                if (p[1].Substring(0, 1) == "'")
                                {
                                    p1isvalue = true;
                                    p[1] = p[1].Substring(1);
                                    p[1] = p[1].Substring(0, p[1].Length - 1);
                                }
                                else if (p[1] == "NULL")
                                    p[1] = null;

                                column_order_struct o = get_column_order(p[0], ref list_spreaded_select_columns);
                                column_order_struct o2 = get_column_order(p[1], ref list_spreaded_select_columns);

                                var ekcik = "";
                                var ekciu = "";
                                var ekcik0 = "";
                                var ekciu0 = "";
                                var ekcik1 = "";
                                var ekciu1 = "";

                                if ((o.ci == true && o2.ci == true) || (o.ci == true && p1isvalue) || (p0isvalue && o2.ci == true))
                                {
                                    ekcik = ".ToLower()";
                                    ekciu = ".ToString().ToLower()";
                                    ekcik0 = ".ToLower()";
                                    ekciu0 = ".ToString().ToLower()";
                                    ekcik1 = ".ToLower()";
                                    ekciu1 = ".ToString().ToLower()";

                                    if (p[0] == null)
                                    {
                                        ekcik0 = "";
                                        ekciu0 = "";
                                    }
                                    if (p[1] == null)
                                    {
                                        ekcik1 = "";
                                        ekciu1 = "";
                                    }
                                }

                                String part = "";
                                if (o.table_name != null && o2.table_name != null)
                                {
                                    var type1 = list_spreaded_select_columns[o.indis2].type;
                                    var type2 = list_spreaded_select_columns[o2.indis2].type;
                                    if (type1 == "text" || type2 == "text")
                                        part = "string.Compare(hw[" + o2.indis2 + "]==null?null:hw[" + o.indis2 + "]" + ekciu + ", hw[" + o2.indis2 + "]==null?null:hw[" + o2.indis2 + "]" + ekciu + ") " + op + " 0";
                                    else
                                    {
                                        part = "((hw[" + o.indis2 + "]==null) ? 0 : double.Parse(hw[" + o.indis2 + "].ToString())) ";
                                        part += op + " ";
                                        part += "((hw[" + o2.indis2 + "]==null) ? 0 : double.Parse(hw[" + o2.indis2 + "].ToString()))";
                                    }
                                }
                                else if (o.table_name == null && o2.table_name == null)
                                {
                                    double res = 0;
                                    Int64 res2;

                                    var type1 = double.TryParse(p[0], out res) || Int64.TryParse(p[0], out res2) ? "" : "text";
                                    var type2 = double.TryParse(p[1], out res) || Int64.TryParse(p[1], out res2) ? "" : "text";

                                    if (type1 == "text" || type2 == "text")
                                        part = "string.Compare(" + (p[0] == null ? "null" : "\"" + p[0] + "\"" + ekciu0 + "") + ", " + (p[1] == null ? "null" : "\"" + p[1] + "\"" + ekciu1 + "") + ") " + op + " 0";
                                    else
                                    {
                                        part = p[0] + " ";
                                        part += op + " ";
                                        part += p[1];
                                    }

                                }
                                else if (o.table_name == null)
                                {
                                    double res = 0;
                                    Int64 res2;

                                    var type1 = double.TryParse(p[0], out res) || Int64.TryParse(p[0], out res2) ? "" : "text";
                                    var type2 = list_spreaded_select_columns[o2.indis2].type;

                                    if (type1 == "text" || type2 == "text")
                                        part = "string.Compare(" + (p[0] == null ? "null" : "\"" + p[0] + "\"" + ekciu0 + "\"") +", ((hw[" + o2.indis2 + "]==null) ? null : hw[" + o2.indis2 + "].ToString()" + ekcik + ")) " + op + " 0";
                                    else
                                    {
                                        part = p[0] + " ";
                                        part += op + " ";
                                        part += "((hw[" + o2.indis2 + "]==null) ? 0 : double.Parse(hw[" + o2.indis2 + "].ToString()))";
                                    }

                                }
                                else if (o2.table_name == null)
                                {
                                    double res = 0;
                                    Int64 res2 = 0;

                                    var type1 = list_spreaded_select_columns[o.indis2].type;
                                    var type2 = double.TryParse(p[1], out res) || Int64.TryParse(p[1], out res2) ? "" : "text";

                                    if (type1 == "text" || type2 == "text")
                                        part = "string.Compare(((hw[" + o.indis2 + "]==null) ? null : hw[" + o.indis2 + "].ToString()" + ekcik + "), " + (p[1] == null ? "null" : "\"" + p[1] + "\"" + ekciu1 + "") + ") " + op + " 0";
                                    else
                                    {
                                        part += "((hw[" + o.indis2 + "]==null) ? 0 : double.Parse(hw[" + o.indis2 + "].ToString())) ";
                                        part += op + " ";
                                        part += p[1];
                                    }

                                }

                                parts2.Add(part);
                            }
                        }

                        havingstr = Regex.Replace(havingstr, "&(?=(?:[^']*'[^']*')*[^']*$)", "&&");
                        havingstr = Regex.Replace(havingstr, "\\|(?=(?:[^']*'[^']*')*[^']*$)", "||");

                        for (var i = 0; i < parts.Count; ++i)
                            havingstr = havingstr.Replace(parts[i], parts2[i]);

                        sql += ".Where(hw => "+ havingstr +")";

                        var sstr = "";

                        for (int i = 0; i < list_spreaded_select_columns.Count; ++i)
                            sstr += "s2["+ i +"], ";

                        sstr = sstr.Substring(0, sstr.Length - 2);

                        sql += ".Select(s2 => new List<Object> {"+ sstr +"})";
                    }
                    
                }

                //orderby
                if (orderby_list.Count > 0)
                {
                    for (var i = 0; i < orderby_list.Count; ++i)
                    {
                        var oparts = orderby_list[i].InnerText.Split(',');
                        var asc_desc = orderbytype_list.Count > i ? orderbytype_list[i].InnerText : "asc";
                        column_order_struct o = get_column_order(oparts[0].Trim(), ref list_spreaded_select_columns);
                        if (o.table_name == null) return set_select_error("Invalid column named `" + oparts[0].Trim() + "` in order by clause!");

                        if (i == 0)
                        {
                            if (asc_desc == "asc")
                                sql += ".OrderBy(x => x[" + o.indis2 + "])";
                            else
                                sql += ".OrderByDescending(x => x[" + o.indis2 + "])";
                        }
                        else 
                        {
                            if (asc_desc == "asc")
                                sql += ".ThenBy(x => x[" + o.indis2 + "])";
                            else
                                sql += ".ThenByDescending(x => x[" + o.indis2 + "])";
                        }

                        for (var k = 1; k < oparts.Length; ++k)
                        {
                            o = get_column_order(oparts[k].Trim(), ref list_spreaded_select_columns);
                            if (o.table_name == null) return set_select_error("Invalid column named `" + oparts[k].Trim() + "` in order by clause!");
                            if (asc_desc == "asc")
                                sql += ".ThenBy(x => x[" + o.indis2 + "])";
                            else
                                sql += ".ThenByDescending(x => x[" + o.indis2 + "])";
                        }
                    }

                    sql += "\n";
                } //orderby:son

                //File.WriteAllText("sql.txt", sql);
                EvalCSCode e = new EvalCSCode();
                
                try
                {
                    whole_records[0] = e.EvalWithParams(
                    "var culture_info = System.Globalization.CultureInfo.CreateSpecificCulture(c);\n" +
                    "culture_info.NumberFormat.NumberDecimalSeparator = \".\";\n" +
                    "System.Threading.Thread.CurrentThread.CurrentCulture = culture_info;\n" +
                    "return " + sql + ";\n",
                    "List<List<List<Object>>> tables, string c", whole_records, cultureinfostr).ToList();
                }
                catch (Exception ex)
                {
                    return set_select_error(ex.Message);
                }
            }

            //SON İŞLEMLER
            //geri dönüş değişkenini doldur:alan isimleri ve türleri
            var ret_columns = new List<Object>();
            var ret_types = new List<Object>();
            for (var i = 0; i < list_spreaded_select_columns.Count; ++i) { 
                var name = list_spreaded_select_columns[i].alias;
                if (name == null) name = list_spreaded_select_columns[i].name;
                ret_columns.Add(name);

                var type = list_spreaded_select_columns[i].type +"";
                if (list_spreaded_select_columns[i].ai == true)
                    type += ",AUTOINCREMENT";
                if (list_spreaded_select_columns[i].ci == true)
                    type += ",CASEINSENSITIVE";
                ret_types.Add(type);
            }
            l_ret.Add(ret_columns);
            l_ret.Add(ret_types);

            if (list_spreaded_select_columns.Count == 1 && list_spreaded_select_columns[0].function == "count" && list_spreaded_select_columns[0].name == "*")
            {
                var ret = new List<Object>();
                var ret2 = new List<List<Object>>();
                if (whole_records.Count == 0)
                    return set_select_error("An unspecified error has occured!");

                ret.Add(whole_records[0].Count);
                ret2.Add(ret);
                whole_records[0] = ret2;
            }

            int start = 0;
            int length = 1000000000;

            if (limit_list.Count > 0) {
                var lp = limit_list[0].InnerText.Split(',');
             
                if (lp.Count() > 1) {
                    lp[0] = lp[0].Trim();
                    lp[1] = lp[1].Trim();
                }

                if (lp.Count() == 1)
                    length = Convert.ToInt32(lp[0]);
                else {
                    start = Convert.ToInt32(lp[0]);
                    length = Convert.ToInt32(lp[1]);
                }
            }

            //geri dönüş değişkenini doldur:kayıtlar
            try
            {
                if (whole_records.Count > 0)
                    for (var i = start; i < whole_records[0].Count && i < length + start; ++i)
                    {
                        l_ret.Add(whole_records[0][i]);
                    }
            }
            catch (Exception ex) {
                return set_select_error("Unspecified error occured!");
            }

            cache_slots_rw_lock.EnterWriteLock();
            var rn = "";
            do {
                rn = random_number();

            } while (cache_slots.ContainsKey(rn));

            CCacheSlot cs = new CCacheSlot();
            cs.curr_record_indis = 2;
            cs.records.AddRange(l_ret);
            cs.last_access_time = DateTime.Now;

            cache_slots.Add(rn, cs);

            cache_slots_rw_lock.ExitWriteLock();

            return set_select_error(rn);
        }

        String f_update(String db, XmlDocument pdoc)
        {
            var l_ret = new List<List<Object>>();

            String tablename = pdoc.SelectNodes("//root/update/table_name")[0].InnerText;
            XmlNodeList clist = pdoc.SelectNodes("//root/update/col");
            XmlNodeList vlist = pdoc.SelectNodes("//root/values/val");
            XmlNodeList join_tablelist = pdoc.SelectNodes("//root/join/table_name");
            XmlNodeList join_onlist = pdoc.SelectNodes("//root/join/on");
            XmlNodeList join_typelist = pdoc.SelectNodes("//root/join/type");
            XmlNodeList whereexpression_list = pdoc.SelectNodes("//root/where/expression");
            XmlNodeList whereewithindex_list = pdoc.SelectNodes("//root/where/withindex");
            XmlNodeList orderby_list = pdoc.SelectNodes("//root/orderby");
            XmlNodeList orderbytype_list = pdoc.SelectNodes("//root/orderbytype");
            XmlNodeList groupby_list = pdoc.SelectNodes("//root/groupby");
            XmlNodeList limit_list = pdoc.SelectNodes("//root/limit");

            if (clist.Count == 0)
                return "Select command must contain least one column!";

            if (clist.Count != vlist.Count)
                return "Columns count and Values count must be equal!";

            if (whereexpression_list.Count > 2)
                return "Select command can't contain more than 2 where clauses!";
        
            
            if (whereexpression_list.Count == 2)
            {
                if (whereewithindex_list[0].InnerText == "True" && whereewithindex_list[1].InnerText == "True" || whereewithindex_list[0].InnerText == "False" && whereewithindex_list[1].InnerText == "False")
                {

                    return "Two where clauses can't be same type!";
                    
                }
                else if (whereewithindex_list[1].InnerText == "True")
                {

                    return "Second where clause can't be its withindex parameter is True!";
                    
                }
            }
            if (groupby_list.Count > 0)
            {

                return ("Update command can't contain a groupby clause!");
                
            }
            if (limit_list.Count > 1)
            {

                return ("Update command can't contain a limit clause!");
                
            }
            if (join_tablelist.Count > 0)
            {

                return ("Update command can't contain a join clause!");
                
            }
            


            var list_select_columns = new List<select_column_struct>();


            for (var i = 0; i < clist.Count; ++i)
            {
                var select_column = new select_column_struct();

                bool space = clist[i].InnerText.Contains(" ");
                if (space)
                    return ((i + 1) + ". column is bad format! Code:1");
                else
                    select_column.name = clist[i].InnerText;

                list_select_columns.Add(select_column);
            }

            var list_tables = new List<mytable>();

            var firsttable = gettable(db, tablename);

            if (firsttable == null) return (tablename + " doesn't exist!");

            list_tables.Add(firsttable);

            
            var list_spreaded_select_columns = new List<select_column_struct>();
            for (var k = 0; k < list_tables.Count; ++k)
            {
                for (var j = 0; j < list_tables[k].columns_info.Count; ++j)
                {
                    select_column_struct spreaded_column = new select_column_struct();
                    spreaded_column.ai = list_tables[k].columns_info[j].ai;
                    spreaded_column.ci = list_tables[k].columns_info[j].ci;
                    spreaded_column.name = list_tables[k].columns_info[j].name;
                    spreaded_column.type = list_tables[k].columns_info[j].type;
                    spreaded_column.table = list_tables[k];
                    var curr_indexes = getindexes(db, list_tables[k].table_name);
                    for (var g = 0; g < curr_indexes.Count; ++g)
                        for (var s = 0; s < curr_indexes[g].sub_indexes.Count; ++s)
                            if (spreaded_column.name == curr_indexes[g].sub_indexes[s].column_name)
                            {
                                spreaded_column.index = curr_indexes[g].sub_indexes[s];
                                goto CIK;
                            }
                CIK:
                    list_spreaded_select_columns.Add(spreaded_column);
                }
            }

            // kolonlar duplicate mi?
            for (var i = 0; i < list_spreaded_select_columns.Count; ++i)
            {

                String curr_name = list_spreaded_select_columns[i].alias;
                if (curr_name == null || curr_name == "") curr_name = list_spreaded_select_columns[i].name;

                for (var k = i + 1; k < list_spreaded_select_columns.Count; ++k)
                {
                    String curr_name2 = list_spreaded_select_columns[k].alias;
                    if (curr_name2 == null || curr_name2 == "") curr_name2 = list_spreaded_select_columns[k].name;

                    if (curr_name == curr_name2)
                        return ("The column named `" + curr_name + "` is ambigious!");
                }
            }

            // ilk where deyimi indexle mi alakalı?
            var kv_index_queried_columns = new List<select_column_struct>();

            if (whereexpression_list.Count > 0 && whereewithindex_list[0].InnerText == "True")
            {
                String indexed_where_exp = whereexpression_list[0].InnerText;
                if (indexed_where_exp == "") return ("First where clause is empty!");

                var testor = Regex.Split(indexed_where_exp, "\\|\\|(?=(?:[^']*'[^']*')*[^']*$)");
                if (testor.Length > 1) return ("|| operator is unsopperted in indexed where() function!");

                var parts = Regex.Split(indexed_where_exp, "&&(?=(?:[^']*'[^']*')*[^']*$)");

                for (var i = 0; i < parts.Count(); ++i)
                {
                    var m = parts[i];
                    var curr_op = "";
                    var pp = Regex.Split(m, " not like (?=(?:[^']*'[^']*')*[^']*$)");

                    if (pp.Length < 2)
                    {
                        pp = Regex.Split(m, "==(?=(?:[^']*'[^']*')*[^']*$)");
                        if (pp.Length < 2)
                        {
                            pp = Regex.Split(m, " like (?=(?:[^']*'[^']*')*[^']*$)");
                            if (pp.Length < 2)
                            {
                                pp = Regex.Split(m, "!=(?=(?:[^']*'[^']*')*[^']*$)");
                                if (pp.Length < 2)
                                {
                                    pp = Regex.Split(m, ">=(?=(?:[^']*'[^']*')*[^']*$)");
                                    if (pp.Length < 2)
                                    {
                                        pp = Regex.Split(m, "<=(?=(?:[^']*'[^']*')*[^']*$)");
                                        if (pp.Length < 2)
                                        {
                                            pp = Regex.Split(m, ">(?=(?:[^']*'[^']*')*[^']*$)");
                                            if (pp.Length < 2)
                                            {
                                                pp = Regex.Split(m, "<(?=(?:[^']*'[^']*')*[^']*$)");
                                                if (pp.Length < 2)
                                                    return ("There is invalid operator at first where expression!");
                                                else
                                                    curr_op = "<";
                                            }
                                            else
                                                curr_op = ">";
                                        }
                                        else
                                            curr_op = "<=";
                                    }
                                    else
                                        curr_op = ">=";
                                }
                                else
                                    curr_op = "!=";
                            }
                            else
                                curr_op = "like";
                        }
                        else
                            curr_op = "=";
                    }
                    else
                        curr_op = "not like";

                    String curr_column = pp[0].Trim();
                    String curr_value = pp[1].Trim();

                    var founded = false;

                    for (var k = 0; k < list_tables.Count; ++k)
                    {
                        for (var j = 0; j < list_tables[k].columns_info.Count; ++j)
                        {
                            if (curr_column == list_tables[k].columns_info[j].name)
                            {
                                founded = true;
                                select_column_struct column = new select_column_struct();
                                column.ai = list_tables[k].columns_info[j].ai;
                                column.ci = list_tables[k].columns_info[j].ci;
                                column.name = list_tables[k].columns_info[j].name;
                                column.type = list_tables[k].columns_info[j].type;
                                column.table = list_tables[k];
                                column.value = curr_value;
                                column.op = curr_op;
                                column.index = null;
                                var curr_indexes = getindexes(db, list_tables[k].table_name);
                                for (var g = 0; g < curr_indexes.Count; ++g)
                                    for (var s = 0; s < curr_indexes[g].sub_indexes.Count; ++s)
                                        if (column.name == curr_indexes[g].sub_indexes[s].column_name)
                                        {
                                            column.index = curr_indexes[g].sub_indexes[s];
                                            goto CIK4;
                                        }
                            CIK4:
                                kv_index_queried_columns.Add(column);
                            }
                        }
                    }

                    if (!founded) return ("The column named ´" + curr_column + "´ was not founded in related table! Code:2");
                }
            }

            //dosya gostericisi pozisyonlarını index dosyalarından sorgula
            var list_cursor_positions = new List<List<Int64>>();

            for (var i = 0; i < kv_index_queried_columns.Count; ++i)
            {
                if (kv_index_queried_columns[i].index == null)
                    return (kv_index_queried_columns[i].name + " has no index!");

                var cursor_positions = new List<Int64>();

                

                String index_key = kv_index_queried_columns[i].value;
                String op = kv_index_queried_columns[i].op;
                var ci = kv_index_queried_columns[i].ci;

                var sql = "";
                if (index_key == "NULL")
                {
                    if (op == "=")
                        sql = "SELECT * FROM keyvalue WHERE [key] ISNULL";
                    else if (op == "!=")
                        sql = "SELECT * FROM keyvalue WHERE NOT ([key] ISNULL)";
                    else
                        sql = "SELECT * FROM keyvalue WHERE [key] " + op + " NULL";
                }
                else if (ci == true && (op == "like" || op == "not like"))
                    sql = "SELECT * FROM keyvalue WHERE TOUPPER([key]) " + op + " TOUPPER(" + index_key + ")";
                else
                    sql = "SELECT * FROM keyvalue WHERE [key] " + op + " " + index_key;


                var cmd = new SQLiteCommand(sql, kv_index_queried_columns[i].index.index_file);
                SQLiteDataReader r;
                try
                {
                    r = cmd.ExecuteReader();
                }
                catch (Exception e)
                {
                    cmd.Dispose();
                    
                    return e.Message;
                }

                while (r.Read())
                {
                    cursor_positions.Add((long)r["value"]);
                }

                r.Close();
                cmd.Dispose();
                
                

                list_cursor_positions.Add(cursor_positions);
            }

            //gösterici pozisyonlarının kesişimlerini al
            var list_cursor_positions_son = new List<List<Int64>>();

            for (var i = 0; i < kv_index_queried_columns.Count; ++i)
            {
                var curr_cursor_positions = list_cursor_positions[i];
                for (var k = 0; k < kv_index_queried_columns.Count; ++k)
                {
                    if (i == k)
                        continue;
                    if (kv_index_queried_columns[i].table.table_name == kv_index_queried_columns[k].table.table_name)
                    {
                        curr_cursor_positions = curr_cursor_positions.Intersect(list_cursor_positions[k]).ToList();
                    }
                }
                list_cursor_positions_son.Add(curr_cursor_positions);
            }

            var list_cursor_positions_son2 = list_cursor_positions_son;

            //tablolar için tanımlı olmayanlara pozisyon değeri olarak -1 ata
            var list_cursor_positions_son3 = new List<List<Int64>>();

            for (var i = 0; i < list_tables.Count; ++i)
            {
                var poses = new List<Int64>();
                var found = false;
                for (var k = 0; k < kv_index_queried_columns.Count; ++k)
                {
                    if (list_tables[i].table_name == kv_index_queried_columns[k].table.table_name)
                    {
                        found = true;
                        for (var j = 0; j < list_cursor_positions_son2[k].Count; ++j)
                            poses.Add(list_cursor_positions_son2[k][j]);
                    }
                }

                //if (!found) poses.Add(-1);
                list_cursor_positions_son3.Add(poses);
            }

            for (var i = 0; i < list_cursor_positions_son3.Count; ++i)
            {
                if (list_cursor_positions_son3[i].Count == 0)
                {
                    var curr_tablename = list_tables[i].table_name;
                    var found = false;

                    for (var k = 0; k < kv_index_queried_columns.Count; ++k)
                        if (curr_tablename == kv_index_queried_columns[k].table.table_name)
                        {
                            found = true;
                            break;
                        }

                    if (!found) list_cursor_positions_son3[i].Add(-1);
                }
            }

            for (var i = 0; i < list_cursor_positions_son3.Count; ++i)
                list_cursor_positions_son3[i] = list_cursor_positions_son3[i].Distinct().ToList();

            var whole_records = new List<List<List<Object>>>();
            var whole_records2 = new List<List<List<Object>>>();

            // dosya işlemleri
            int size = 0;
            List<Stream> fps = new List<Stream>();
            for (var i = 0; i < list_tables.Count; ++i)
            {
                
                Stream fp = File.Open(list_tables[i].data_file_path, FileMode.Open, FileAccess.Read, FileShare.Read);
                fps.Add(fp);
            }

            for (var i = 0; i < list_cursor_positions_son3.Count; ++i)
            {
                var curr_table_records = new List<List<Object>>();
                if (list_cursor_positions_son3[i].Count == 0)
                    continue;

                if (list_cursor_positions_son3[i][0] == -1) // tüm kayıtları getir
                {
                    while (true)
                    {
                        var curr_record = new List<Object>();
                        Int64 curr_pos = fps[i].Position;

                        var deleted_flag = new byte[1];
                        if (fps[i].Read(deleted_flag, 0, 1) == 0)
                            break;

                        if (deleted_flag[0] == 1)
                        {
                            for (var k = 0; k < list_tables[i].columns_info.Count; ++k)
                            {
                                var null_flag = new byte[1];
                                fps[i].Read(null_flag, 0, 1);
                                if (null_flag[0] == 1)
                                {
                                    fps[i].Seek(1, SeekOrigin.Current);
                                    continue;
                                }

                                switch (list_tables[i].columns_info[k].type)
                                {
                                    case "tinyint": size = sizeof(sbyte); break;
                                    case "smallint": size = sizeof(Int16); break;
                                    case "int": size = sizeof(Int32); break;
                                    case "bigint": size = sizeof(Int64); break;
                                    case "text":
                                        var bytesofsize = new byte[sizeof(int)];
                                        fps[i].Read(bytesofsize, 0, sizeof(int));
                                        size = BitConverter.ToInt32(bytesofsize, 0);
                                        break;
                                    case "double": size = sizeof(double); break;
                                }
                                fps[i].Seek(size, SeekOrigin.Current);
                            }
                            continue;
                        }

                        var curr_column_value = new Object();
                        // deleted flag = 0, verileri çek

                        for (var j = 0; j < list_tables[i].columns_info.Count; ++j)
                        {
                            var null_flag = new byte[1];
                            fps[i].Read(null_flag, 0, 1);
                            if (null_flag[0] == 1)
                            {
                                fps[i].Seek(1, SeekOrigin.Current);
                                curr_record.Add(null);
                                continue;
                            }

                            byte[] curr_column_bytes;

                            switch (list_tables[i].columns_info[j].type)
                            {
                                case "tinyint":
                                    size = sizeof(sbyte);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = curr_column_bytes[0];
                                    break;
                                case "smallint":
                                    size = sizeof(Int16);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt16(curr_column_bytes, 0);
                                    break;
                                case "int": size =
                                    sizeof(Int32);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt32(curr_column_bytes, 0);
                                    break;
                                case "bigint":
                                    size = sizeof(Int64);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt64(curr_column_bytes, 0);
                                    break;
                                case "text":
                                    var bytesofsize = new byte[sizeof(int)];
                                    fps[i].Read(bytesofsize, 0, sizeof(int));
                                    size = BitConverter.ToInt32(bytesofsize, 0);

                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);

                                    curr_column_value = Encoding.Unicode.GetString(curr_column_bytes, 0, size);
                                    break;
                                case "double":
                                    size = sizeof(double);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToDouble(curr_column_bytes, 0);
                                    break;

                            }

                            for (var k = 0; k < list_spreaded_select_columns.Count; ++k)
                            {
                                if (list_tables[i].columns_info[j].name == list_spreaded_select_columns[k].name)
                                {
                                    curr_record.Add(curr_column_value);
                                }
                            }
                        }

                        curr_record.Add(curr_pos);
                        curr_table_records.Add(curr_record);
                    }

                }
                else // indexten gelen kayıtları getir
                {
                    for (var g = 0; g < list_cursor_positions_son3[i].Count; ++g)
                    {
                        var curr_record = new List<Object>();

                        Int64 curr_pos = list_cursor_positions_son3[i][g];
                        
                        fps[i].Seek(curr_pos, SeekOrigin.Begin);

                        var deleted_flag = new byte[1];
                        fps[i].Read(deleted_flag, 0, 1);

                        if (deleted_flag[0] == 1)
                            continue;

                        var curr_column_value = new Object();
                        // deleted flag = 0, verileri çek

                        for (var j = 0; j < list_tables[i].columns_info.Count; ++j)
                        {
                            var null_flag = new byte[1];
                            fps[i].Read(null_flag, 0, 1);
                            if (null_flag[0] == 1)
                            {
                                fps[i].Seek(1, SeekOrigin.Current);
                                curr_record.Add(null);
                                continue;
                            }

                            byte[] curr_column_bytes;

                            switch (list_tables[i].columns_info[j].type)
                            {
                                case "tinyint":
                                    size = sizeof(sbyte);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = curr_column_bytes[0];
                                    break;
                                case "smallint":
                                    size = sizeof(Int16);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt16(curr_column_bytes, 0);
                                    break;
                                case "int": size =
                                    sizeof(Int32);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt32(curr_column_bytes, 0);
                                    break;
                                case "bigint":
                                    size = sizeof(Int64);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt64(curr_column_bytes, 0);
                                    break;
                                case "text":
                                    var bytesofsize = new byte[sizeof(int)];
                                    fps[i].Read(bytesofsize, 0, sizeof(int));
                                    size = BitConverter.ToInt32(bytesofsize, 0);

                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);

                                    curr_column_value = Encoding.Unicode.GetString(curr_column_bytes, 0, size);
                                    break;
                                case "double":
                                    size = sizeof(double);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToDouble(curr_column_bytes, 0);
                                    break;

                            }

                            for (var k = 0; k < list_spreaded_select_columns.Count; ++k)
                            {
                                if (list_tables[i].columns_info[j].name == list_spreaded_select_columns[k].name)
                                {
                                    curr_record.Add(curr_column_value);
                                }
                            }
                        }

                        curr_record.Add(curr_pos);
                        curr_table_records.Add(curr_record);
                    }
                }

                whole_records.Add(curr_table_records);
            }

            for (var i = 0; i < list_tables.Count; ++i)
            {
                fps[i].Close();
                
            }

      
            //linqtosql sorguları
            var on_exp_list = new List<String>();
            for (var i = 0; i < join_onlist.Count; ++i)
            {
                var curr_on = join_onlist[i].InnerText;
                var parts = Regex.Split(curr_on, "==(?=(?:[^']*'[^']*')*[^']*$)");
                if (parts.Length < 2) return ((i + 1) + ". on expression must contain == operator!");
                var column1 = parts[0].Trim();
                var column2 = parts[1].Trim();

                var order1 = get_column_order(column1, ref list_spreaded_select_columns);
                var order2 = get_column_order(column2, ref list_spreaded_select_columns);

                var type = join_typelist[i].InnerText;

                if (type == "inner")
                {
                    column1 = order1.table_name + "_sl[" + order1.indis + "]";
                    column2 = order2.table_name + "_sl[" + order2.indis + "]";
                }
                else
                {
                    column1 = order1.table_name + "_sl[" + order1.indis + "]";
                    column2 = "lr" + (i + 1) + "_sl[" + order2.indis + "]";
                    if (list_tables[i + 1].table_name != order2.table_name)
                        return (i + 1) + ". `on` parameter's right table's column name is invalid!";
                }
                if (order1.ci == true && order2.ci == true)
                {
                    column1 += ".ToString().ToLower()";
                    column2 += ".ToString().ToLower()";
                }
                on_exp_list.Add(column1 + " equals " + column2);
            }

            String where_exp_without_index = "";

            if (whereexpression_list.Count == 1 && whereewithindex_list[0].InnerText == "False")
            {
                where_exp_without_index = whereexpression_list[0].InnerText;
            }
            else if (whereexpression_list.Count == 2 && whereewithindex_list[1].InnerText == "False")
            {
                where_exp_without_index = whereexpression_list[1].InnerText;
            }

            if (list_tables.Count > 1 || where_exp_without_index != "" || groupby_list.Count > 0 || orderby_list.Count > 0)
            {

                var sql = "(\nfrom " + list_tables[0].table_name + "_sl in tables[0]\n";
                for (var i = 1; i < list_tables.Count; ++i)
                {
                    if (join_typelist[i - 1].InnerText == "inner")
                        sql += "join " + list_tables[i].table_name + "_sl in tables[" + i + "] on " + on_exp_list[i - 1] + "\n";
                    else
                    {
                        sql += "join lr" + i + "_sl in tables[" + i + "] on " + on_exp_list[i - 1] + " into lrs" + i + "\n";
                        sql += "from " + list_tables[i].table_name + "_sl in lrs" + i + ".DefaultIfEmpty()\n";
                    }
                }
                // where exp çözümle

                if (where_exp_without_index != "")
                    if (where_exp_without_index != "")
                    {
                        where_exp_without_index = Regex.Replace(where_exp_without_index, "&&(?=(?:[^']*'[^']*')*[^']*$)", "&");
                        where_exp_without_index = Regex.Replace(where_exp_without_index, "\\|\\|(?=(?:[^']*'[^']*')*[^']*$)", "|");

                        var mc = Regex.Split(where_exp_without_index, "[&|](?=(?:[^']*'[^']*')*[^']*$)");

                        var parts = new List<String>();

                        foreach (String m in mc)
                        {
                            var sm = Regex.Replace(m, "\\((?=(?:[^']*'[^']*')*[^']*$)", "");
                            sm = Regex.Replace(sm, "\\)(?=(?:[^']*'[^']*')*[^']*$)", "");
                            sm = sm.Trim();
                            parts.Add(sm);
                        }

                        parts = (from s in parts
                                 orderby s.Length descending
                                 select s).ToList();
                        var parts2 = new List<String>();

                        for (var i = 0; i < parts.Count; ++i)
                        {
                            var oper = "not like";
                            var p = Regex.Split(parts[i], " not like (?=(?:[^']*'[^']*')*[^']*$)");
                            if (p.Length == 1)
                            {
                                p = Regex.Split(parts[i], " like (?=(?:[^']*'[^']*')*[^']*$)");
                                if (p.Length > 1) oper = "like";
                            }
                            if (p.Length > 1)
                            {
                                p[0] = p[0].Trim();
                                p[1] = p[1].Trim();

                                if (p[0] == "" || p[1] == "")
                                    return "Statement was bad format!";

                                var p0isvalue = false;
                                var p1isvalue = false;

                                if (p[0].Substring(0, 1) == "'")
                                {
                                    p0isvalue = true;
                                    p[0] = p[0].Substring(1);
                                    if (p[0].Length > 0 && p[0].Substring(p[0].Length - 1, 1) == "'")
                                        p[0] = p[0].Substring(0, p[0].Length - 1);
                                }
                                else if (p[0] == "NULL")
                                    p[0] = null;

                                if (p[1].Substring(0, 1) == "'")
                                {
                                    p1isvalue = true;
                                    p[1] = p[1].Substring(1);
                                    if (p[1].Length > 0 && p[1].Substring(p[1].Length - 1, 1) == "'")
                                        p[1] = p[1].Substring(0, p[1].Length - 1);
                                }
                                else if (p[1] == "NULL")
                                    p[1] = null;


                                column_order_struct o = get_column_order(p[0], ref list_spreaded_select_columns);
                                column_order_struct o2 = get_column_order(p[1], ref list_spreaded_select_columns);

                                if (p0isvalue) o.table_name = null;
                                if (p1isvalue) o2.table_name = null;

                                var cond = oper == "like" ? "true" : "false";

                                var ekcik = "";
                                var ekciu = "";
                                var ekcik0 = "";
                                var ekciu0 = "";
                                var ekcik1 = "";
                                var ekciu1 = "";

                                if ((o.ci == true && o2.ci == true) || (o.ci == true && p1isvalue) || (p0isvalue && o2.ci == true))
                                {
                                    ekcik = ".ToLower()";
                                    ekciu = ".ToString().ToLower()";
                                    ekcik0 = ".ToLower()";
                                    ekciu0 = ".ToString().ToLower()";
                                    ekcik1 = ".ToLower()";
                                    ekciu1 = ".ToString().ToLower()";

                                    if (p[0] == null)
                                    {
                                        ekcik0 = "";
                                        ekciu0 = "";
                                    }
                                    if (p[1] == null)
                                    {
                                        ekcik1 = "";
                                        ekciu1 = "";
                                    }
                                }

                                String part = "";
                                if (o.table_name != null && o2.table_name != null)
                                    part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "] == null) ? \"\" : " + o2.table_name + "_sl[" + o2.indis + "].ToString()" + ekcik + "), ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "] == null) ? \"\" : " + o.table_name + "_sl[" + o.indis + "].ToString()" + ekcik + ")) == " + cond;
                                else if (o.table_name == null && o2.table_name == null)
                                    part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(\"" + p[1] + "\"" + ekciu1 + ", ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(\"" + p[0] + "\"" + ekciu0 + ")) == " + cond;
                                else if (o.table_name == null)
                                    part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "] == null) ? \"\" : " + o2.table_name + "_sl[" + o2.indis + "].ToString()" + ekcik + "), ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(\"" + p[0] + "\"" + ekciu0 + ") == " + cond;
                                else
                                    part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(\"" + p[1] + "\"" + ekciu1 + ", ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "] == null) ? \"\" : " + o.table_name + "_sl[" + o.indis + "].ToString()" + ekcik + ")) == " + cond;

                                parts2.Add(part);
                            }
                            else
                            {
                                String op = "";

                                p = Regex.Split(parts[i], "==(?=(?:[^']*'[^']*')*[^']*$)");
                                if (p.Length == 2)
                                    op = "==";
                                else
                                {
                                    p = Regex.Split(parts[i], "!=(?=(?:[^']*'[^']*')*[^']*$)");
                                    if (p.Length == 2)
                                        op = "!=";
                                    else
                                    {
                                        p = Regex.Split(parts[i], ">=(?=(?:[^']*'[^']*')*[^']*$)");
                                        if (p.Length == 2)
                                            op = ">=";
                                        else
                                        {
                                            p = Regex.Split(parts[i], "<=(?=(?:[^']*'[^']*')*[^']*$)");
                                            if (p.Length == 2)
                                                op = "<=";
                                            else
                                            {
                                                p = Regex.Split(parts[i], ">(?=(?:[^']*'[^']*')*[^']*$)");
                                                if (p.Length == 2)
                                                    op = ">";
                                                else
                                                {
                                                    p = Regex.Split(parts[i], "<(?=(?:[^']*'[^']*')*[^']*$)");
                                                    if (p.Length == 2)
                                                        op = "<";
                                                    else
                                                    {
                                                        return ("Invalid operator(s) in where clause!");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                p[0] = p[0].Trim();
                                p[1] = p[1].Trim();

                                if (p[0] == "" || p[1] == "")
                                    return "Statement was bad format!";

                                var p0isvalue = false;
                                var p1isvalue = false;

                                if (p[0].Substring(0, 1) == "'")
                                {
                                    p0isvalue = true;
                                    p[0] = p[0].Substring(1);
                                    if (p[0].Length > 0 && p[0].Substring(p[0].Length - 1, 1) == "'")
                                        p[0] = p[0].Substring(0, p[0].Length - 1);
                                }
                                else if (p[0] == "NULL")
                                    p[0] = null;

                                if (p[1].Substring(0, 1) == "'")
                                {
                                    p1isvalue = true;
                                    p[1] = p[1].Substring(1);
                                    p[1] = p[1].Substring(0, p[1].Length - 1);
                                }
                                else if (p[1] == "NULL")
                                    p[1] = null;

                                column_order_struct o = get_column_order(p[0], ref list_spreaded_select_columns);
                                column_order_struct o2 = get_column_order(p[1], ref list_spreaded_select_columns);

                                var ekcik = "";
                                var ekciu = "";
                                var ekcik0 = "";
                                var ekciu0 = "";
                                var ekcik1 = "";
                                var ekciu1 = "";

                                if ((o.ci == true && o2.ci == true) || (o.ci == true && p1isvalue) || (p0isvalue && o2.ci == true))
                                {
                                    ekcik = ".ToLower()";
                                    ekciu = ".ToString().ToLower()";
                                    ekcik0 = ".ToLower()";
                                    ekciu0 = ".ToString().ToLower()";
                                    ekcik1 = ".ToLower()";
                                    ekciu1 = ".ToString().ToLower()";

                                    if (p[0] == null)
                                    {
                                        ekcik0 = "";
                                        ekciu0 = "";
                                    }
                                    if (p[1] == null)
                                    {
                                        ekcik1 = "";
                                        ekciu1 = "";
                                    }
                                }

                                String part = "";
                                if (o.table_name != null && o2.table_name != null)
                                {
                                    var type1 = list_spreaded_select_columns[o.indis2].type;
                                    var type2 = list_spreaded_select_columns[o2.indis2].type;
                                    if (type1 == "text" || type2 == "text")
                                        part = "string.Compare(((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "]==null) ? null : " + o.table_name + "_sl[" + o.indis + "].ToString()" + ekcik + "), ((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "]==null) ? null : " + o2.table_name + "_sl[" + o2.indis + "].ToString()" + ekcik + ")) " + op + " 0";
                                    else
                                    {
                                        part = "((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "]==null) ? 0 : double.Parse(" + o.table_name + "_sl[" + o.indis + "].ToString())) ";
                                        part += op + " ";
                                        part += "((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "]==null) ? 0 : double.Parse(" + o2.table_name + "_sl[" + o2.indis + "].ToString()))";
                                    }
                                }
                                else if (o.table_name == null && o2.table_name == null)
                                {
                                    double res = 0;
                                    Int64 res2;

                                    var type1 = double.TryParse(p[0], out res) || Int64.TryParse(p[0], out res2) ? "" : "text";
                                    var type2 = double.TryParse(p[1], out res) || Int64.TryParse(p[1], out res2) ? "" : "text";

                                    if (type1 == "text" || type2 == "text")
                                        part = "string.Compare(" + (p[0] == null ? "null" : "\"" + p[0] + "\"" + ekciu0 + "") + ", " + (p[1] == null ? "null" : "\"" + p[1] + "\"" + ekciu1 + "") + ") " + op + " 0";
                                    else
                                    {
                                        part = p[0] + " ";
                                        part += op + " ";
                                        part += p[1];
                                    }

                                }
                                else if (o.table_name == null)
                                {
                                    double res = 0;
                                    Int64 res2;

                                    var type1 = double.TryParse(p[0], out res) || Int64.TryParse(p[0], out res2) ? "" : "text";
                                    var type2 = list_spreaded_select_columns[o2.indis2].type;

                                    if (type1 == "text" || type2 == "text")
                                        part = "string.Compare(" + (p[0] == null ? "null" : "\"" + p[0] + "\"" + ekciu0 + "") + ", ((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "]==null) ? null : " + o2.table_name + "_sl[" + o2.indis + "].ToString()" + ekcik + ")) " + op + " 0";
                                    else
                                    {
                                        part = p[0] + " ";
                                        part += op + " ";
                                        part += "((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "]==null) ? 0 : double.Parse(" + o2.table_name + "_sl[" + o2.indis + "].ToString()))";
                                    }

                                }
                                else if (o2.table_name == null)
                                {
                                    double res = 0;
                                    Int64 res2 = 0;

                                    var type1 = list_spreaded_select_columns[o.indis2].type;
                                    var type2 = double.TryParse(p[1], out res) || Int64.TryParse(p[1], out res2) ? "" : "text";

                                    if (type1 == "text" || type2 == "text")
                                        part = "string.Compare(((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "]==null) ? null : " + o.table_name + "_sl[" + o.indis + "].ToString()" + ekcik + "), " + (p[1] == null ? "null" : "\"" + p[1] + "\"" + ekciu1 + "") + ") " + op + " 0";
                                    else
                                    {
                                        part += "((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "]==null) ? 0 : double.Parse(" + o.table_name + "_sl[" + o.indis + "].ToString())) ";
                                        part += op + " ";
                                        part += p[1];
                                    }

                                }

                                parts2.Add(part);
                            }
                        }

                        where_exp_without_index = Regex.Replace(where_exp_without_index, "&(?=(?:[^']*'[^']*')*[^']*$)", "&&");
                        where_exp_without_index = Regex.Replace(where_exp_without_index, "\\|(?=(?:[^']*'[^']*')*[^']*$)", "||");

                        for (var i = 0; i < parts.Count; ++i)
                            where_exp_without_index = where_exp_without_index.Replace(parts[i], parts2[i]);

                        sql += "where " + where_exp_without_index + "\n";
                    }

                var new_str = "";

                for (var i = 0; i < list_spreaded_select_columns.Count; ++i)
                {
                    if (list_spreaded_select_columns[i].name == "*")
                        new_str += "null,";
                    else
                    {
                        var o = get_column_order(list_spreaded_select_columns[i].name, ref list_spreaded_select_columns);
                        new_str += "(" + o.table_name + "_sl==null?null:" + o.table_name + "_sl[" + o.indis + "]),";
                    }
                }

                new_str += list_spreaded_select_columns[0].table.table_name + "_sl[" + (list_spreaded_select_columns.Count) + "],";
                new_str = new_str.Substring(0, new_str.Length - 1);
                sql += "select new List<Object> {" + new_str + "}\n)";

                //orderby
                if (orderby_list.Count > 0)
                {
                    for (var i = 0; i < orderby_list.Count; ++i)
                    {
                        var oparts = orderby_list[i].InnerText.Split(',');
                        var asc_desc = orderbytype_list.Count > i ? orderbytype_list[i].InnerText : "asc";
                        column_order_struct o = get_column_order(oparts[0].Trim(), ref list_spreaded_select_columns);
                        if (o.table_name == null) return ("Invalid column named `" + oparts[0].Trim() + "` in order by clause!");

                        if (i == 0)
                        {
                            if (asc_desc == "asc")
                                sql += ".OrderBy(x => x[" + o.indis2 + "])";
                            else
                                sql += ".OrderByDescending(x => x[" + o.indis2 + "])";
                        }
                        else
                        {
                            if (asc_desc == "asc")
                                sql += ".ThenBy(x => x[" + o.indis2 + "])";
                            else
                                sql += ".ThenByDescending(x => x[" + o.indis2 + "])";
                        }

                        for (var k = 1; k < oparts.Length; ++k)
                        {
                            o = get_column_order(oparts[k].Trim(), ref list_spreaded_select_columns);
                            if (o.table_name == null) return ("Invalid column named `" + oparts[k].Trim() + "` in order by clause!");
                            if (asc_desc == "asc")
                                sql += ".ThenBy(x => x[" + o.indis2 + "])";
                            else
                                sql += ".ThenByDescending(x => x[" + o.indis2 + "])";
                        }
                    }

                    sql += "\n";
                }
                //File.WriteAllText("sql.txt", sql);
                EvalCSCode e = new EvalCSCode();

                try
                {
                    whole_records[0] = e.EvalWithParams(
                    "var culture_info = System.Globalization.CultureInfo.CreateSpecificCulture(c);\n" +
                    "culture_info.NumberFormat.NumberDecimalSeparator = \".\";\n" +
                    "System.Threading.Thread.CurrentThread.CurrentCulture = culture_info;\n" +
                    "return " + sql + ";\n",
                    "List<List<List<Object>>> tables, string c", whole_records, cultureinfostr).ToList();

                    //MessageBox.Show(sonuc.Count.ToString());
                }
                catch (Exception ex)
                {
                    //MessageBox.Show(ex.Message);
                    return ex.Message;
                }

            }

            whole_records2.Add(new List<List<Object>>());

            for (var i = 0; i < whole_records[0].Count; ++i)
            {
                var new_list = new List<Object>();
                new_list = whole_records[0][i];
                whole_records2[0].Add(new_list);
            }
          
            //dosya işlemleri
            if (whole_records.Count > 0)
            {
                try
                {
                    if (whole_records[0].Count == 0)
                        return "0";
                }
                catch (Exception ex) {
                    return "0";
                }

                for (var i = 0; i < list_select_columns.Count; ++i)
                {
                    for (var k = 0; k < list_tables[0].columns_info.Count; ++k)
                    {
                        if (list_tables[0].columns_info[k].name == list_select_columns[i].name && list_tables[0].columns_info[k].ai == true)
                        {
                            return "Column named `" + list_select_columns[i].name + "` is IDENTITY column and it can't be updated!";
                        }
                    }

                    var willupdateorder = get_column_order(list_select_columns[i].name, ref list_spreaded_select_columns);
                    var datap = Regex.Split(vlist[i].InnerText, "\\+(?=(?:[^']*'[^']*')*[^']*$)");
                    datap[0] = datap[0].Trim();
                    var datatype = "";

                    var found = false;
                    var foundindis = -1;
                    for (var k = 0; k < list_tables[0].columns_info.Count; ++k) {
                        if (list_tables[0].columns_info[k].name == datap[0]) {
                            datatype = list_tables[0].columns_info[k].type;
                            found = true;
                            foundindis = k;
                            break;
                        }
                    }

                    var data = "";
                    var data2 = "";
                    
                    if (datap.Count() > 1)
                    {
                        data2 = datap[1].Trim();
                        if (data2 == "NULL")
                            data2 = null;
                        else if (data2.Substring(0, 1) == "'")
                        {
                            data2 = data2.Substring(1);
                            if (data2.Length > 0 && data2.Substring(data2.Length - 1, 1) == "'")
                                data2 = data2.Substring(0, data2.Length - 1);
                        }
                        else
                            return "Concatanation value is bad format!";
                    }

                    if (found == false)
                    {
                        data = datap[0];
                        if (data == "NULL")
                            data = null;
                        else if (data.Substring(0, 1) == "'")
                        {
                            data = data.Substring(1);
                            if (data.Length > 0 && data.Substring(data.Length - 1, 1) == "'")
                                data = data.Substring(0, data.Length - 1);
                        }
                        datatype = "text";
                    }
                   
                    var rec_indis = list_spreaded_select_columns.Count;

                    for (var k = 0; k < whole_records[0].Count; ++k) {
                                var j = k;
                            
                                var sondata1 = "";
                                var sondata2 = "";
                                var datatoplam = "";
                                if (found)
                                    sondata1 = whole_records2[0][j][foundindis].ToString();
                                else
                                    sondata1 = data;
                                sondata2 = data2;

                                switch (datatype) { 
                                    case "text":
                                        datatoplam = sondata1 + sondata2;
                                        break;
                                    case "double":
                                        try
                                        {
                                            datatoplam = (double.Parse(sondata1) + double.Parse(sondata2)).ToString();
                                        }
                                        catch (Exception ex) {
                                            return ex.Message;
                                        }
                                        break;
                                    case "tinyint":
                                    case "smallint":
                                    case "int":
                                    case "bigint":
                                        try
                                        {
                                            datatoplam = (Int64.Parse(sondata1) + Int64.Parse(sondata2)).ToString();
                                        }
                                        catch (Exception ex) {
                                            return ex.Message;
                                        }
                                        break;
                                }

                                whole_records[0][k][willupdateorder.indis] = datatoplam;
                    }
                }

                //insert to file
                var begin_update = "B";
                var end_update = "E";

                byte[] begin_update_bytes = Encoding.ASCII.GetBytes(begin_update);
                byte[] end_update_bytes = Encoding.ASCII.GetBytes(end_update);

                mytable curr_table = gettable(db, list_tables[0].table_name);

                byte[] update_records_count_bytes = BitConverter.GetBytes((Int64) whole_records[0].Count);

                

                Stream logfp = File.Open(curr_table.update_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
                logfp.Seek(0, SeekOrigin.End);
                logfp.Write(begin_update_bytes, 0, begin_update_bytes.Count());
                logfp.Write(update_records_count_bytes, 0, update_records_count_bytes.Count());

                for (var i = 0; i < whole_records[0].Count; ++i)
                {
                    byte[] rec_bytes = BitConverter.GetBytes(Int64.Parse(whole_records[0][i][list_spreaded_select_columns.Count].ToString()));
                    logfp.Write(rec_bytes, 0, rec_bytes.Count());
                }

                var rollback = new List<rollback_struct>();

                for (var i = 0; i < list_spreaded_select_columns.Count; ++i) {
                    if (list_spreaded_select_columns[i].index != null) {
                        
                        

                        var cmd = new SQLiteCommand();
                        cmd.Connection = list_spreaded_select_columns[i].index.index_file;

                        var rb = new rollback_struct();
                        rb.conn = list_spreaded_select_columns[i].index.index_file;
                        

                        for (var k = 0; k < whole_records[0].Count; ++k) {
                            var sql = "SELECT * FROM keyvalue WHERE value = " + whole_records[0][k][list_spreaded_select_columns.Count].ToString();
                            cmd.CommandText = sql;
                            SQLiteDataReader r = cmd.ExecuteReader();
                            r.Read();
                            rb.key = r["key"] == null ? null : r["key"].ToString();
                            rb.value = r["value"].ToString();
                            r.Close();

                            rb.type = "insert";
                            rollback.Add(rb);

                            sql = "DELETE FROM keyvalue WHERE value = " + whole_records[0][k][list_spreaded_select_columns.Count].ToString();
                            cmd.CommandText = sql;
                            cmd.ExecuteNonQuery();
                        }

                        cmd.Dispose();
                        
                        
                    }
                }

                var curr_index = getindexes(db, curr_table.table_name);

                var list_afterkeyslist = new List<List<List<String>>>();

                for (var w = 0; w < whole_records[0].Count; ++w)
                {
                    var afterkeyslist = new List<List<String>>();

                    for (int i = 0; i < curr_index.Count; ++i)
                    {
                        var curr_subindexes = curr_index[i].sub_indexes;
                        var duplicate_count = 0;
                        var afterkeys = new List<String>();

                        for (var k = 0; k < curr_subindexes.Count; ++k)
                        {
                            var curr_index_file = curr_subindexes[k].index_file;

                            var bulunanindis = -1;

                            for (var j = 0; j < curr_table.columns_info.Count; ++j)
                                if (curr_table.columns_info[j].name == curr_subindexes[k].column_name)
                                {
                                    bulunanindis = j;
                                    break;
                                }

                            var curr_value = "";

                            if (whole_records[0][w][bulunanindis] == null)
                                curr_value = "\0";
                            else
                                curr_value = whole_records[0][w][bulunanindis].ToString();

                            afterkeys.Add(curr_value);

                            if (curr_index[i].unique == true)
                            {
                                String index_key = curr_value;
                                var sql = "";
                                if (index_key == "\0")
                                    sql = "SELECT * FROM keyvalue WHERE key ISNULL";
                                else
                                    sql = "SELECT * FROM keyvalue WHERE key = '" + index_key.Replace("'", "''") + "'";

                                

                                var cmd = new SQLiteCommand(sql, curr_index_file);
                                SQLiteDataReader r = cmd.ExecuteReader();

                                if (r.Read())
                                {
                                    duplicate_count++;
                                }
                                r.Close();
                                cmd.Dispose();

                                
                            }
                        }

                        if (curr_index[i].unique == true && duplicate_count > 0 && duplicate_count == curr_subindexes.Count)
                        {
                            var comm = new SQLiteCommand();
                            for (var g = 0; g < rollback.Count; ++g)
                            {
                                if (rollback[g].type == "insert")
                                    continue;

                                
                                var sql = "";

                                if (rollback[g].key == null)
                                    sql = "DELETE FROM keyvalue WHERE key ISNULL AND value = " + rollback[g].value;
                                else
                                    sql = "DELETE FROM keyvalue WHERE key = '" + rollback[g].key.Replace("'", "''") + "' AND value = " + rollback[g].value;

                                //rollback[g].conn.Open();

                                comm.Connection = rollback[g].conn;
                                comm.CommandText = sql;
                                comm.ExecuteNonQuery();

                                //rollback[g].conn.Close();
                                
                            }

                            for (var g = 0; g < rollback.Count; ++g) 
                            {
                                if (rollback[g].type == "delete")
                                    continue;

                                
                                var sql = "";

                                if (rollback[g].key == null)
                                    sql = "INSERT INTO keyvalue VALUES (NULL, " + rollback[g].value + ")";
                                else
                                    sql = "INSERT INTO keyvalue VALUES ('" + rollback[g].key.Replace("'", "''") + "', " + rollback[g].value + ")";

                                //rollback[g].conn.Open();

                                comm.Connection = rollback[g].conn;
                                comm.CommandText = sql;
                                comm.ExecuteNonQuery();

                                //rollback[g].conn.Close();
                                
                            }
                            comm.Dispose();

                            for (var g = 0; g < whole_records[0].Count; ++g)
                            {
                                byte[] rec_bytes = BitConverter.GetBytes(Int64.Parse(whole_records[0][g][list_spreaded_select_columns.Count].ToString()));
                                logfp.Write(rec_bytes, 0, rec_bytes.Count());
                            }

                            logfp.Write(end_update_bytes, 0, end_update_bytes.Count());
                            logfp.Close();
                            
                            return "The index named ´" + curr_index[i].index_name + "´ does not allow duplicate entry!";
                        }
                        else
                        {
                            afterkeyslist.Add(afterkeys);
                        }
                    }

                    //??
                    for (int i = 0; i < curr_index.Count; ++i)
                    {
                        int gost = 0;

                        for (int k = 0; k < curr_index[i].sub_indexes.Count; ++k)
                        {
                            String index_key = afterkeyslist[i][gost++];
                            String recno = whole_records[0][w][list_spreaded_select_columns.Count].ToString();

                            var sql = "";
                            if (index_key == "\0")
                                sql = "INSERT INTO keyvalue VALUES (NULL, "+ recno +")";
                            else
                                sql = "INSERT INTO keyvalue VALUES ('" + index_key.Replace("'", "''") + "', " + recno + ")";

                            var rb2 = new rollback_struct();
                            rb2.type = "delete";
                            rb2.conn = curr_index[i].sub_indexes[k].index_file;
                            
                            rb2.key = index_key == "\0" ? null : index_key;
                            rb2.value = recno;
                            rollback.Add(rb2);

                            
                            
                            var cmd = new SQLiteCommand(sql, curr_index[i].sub_indexes[k].index_file);
                            cmd.ExecuteNonQuery();
                            cmd.Dispose();
                            
                           
                        }
                    }

                    list_afterkeyslist.Add(afterkeyslist);
                }

                var filelens = new List<Int64>();

                Stream st = File.Open("Databases/" + db + "/" + tablename + ".ssd", FileMode.Open, FileAccess.ReadWrite, FileShare.None);

                for (var w = 0; w < whole_records[0].Count; ++w) 
                {
                    st.Seek(0, SeekOrigin.End);
                    long filelen = st.Position;
                    filelens.Add(filelen);

                    byte[] filelen_bytes = BitConverter.GetBytes(filelen);

                    logfp.Write(filelen_bytes, 0, filelen_bytes.Count());

                    

                    byte[] deletedflag = new byte[1];
                    deletedflag[0] = 0;
                    st.Write(deletedflag, 0, 1);

                    for (var i = 0; i < whole_records[0][w].Count-1; ++i)
                    {
                        size = 0;

                        var curr_value = "";
                        if (whole_records[0][w][i] != null)
                        {
                            curr_value = whole_records[0][w][i].ToString();
                        }
                        else
                            curr_value = null;

                        switch (list_spreaded_select_columns[i].type)
                        {
                            case "tinyint": size = sizeof(sbyte); break;
                            case "smallint": size = sizeof(Int16); break;
                            case "int": size = sizeof(Int32); break;
                            case "bigint": size = sizeof(Int64); break;
                            case "text":
                                if (curr_value != null)
                                {
                                    size = Encoding.Unicode.GetBytes(curr_value).Count();
                                }
                                break;
                            case "double": size = sizeof(double); break;
                        }

                        byte[] bytes = new byte[size];

                        if (curr_value != null)
                            switch (list_spreaded_select_columns[i].type)
                            {
                                case "tinyint": bytes[0] = (byte)sbyte.Parse(curr_value); break;
                                case "smallint": bytes = BitConverter.GetBytes(Int16.Parse(curr_value)); break;
                                case "int": bytes = BitConverter.GetBytes(Int32.Parse(curr_value)); break;
                                case "bigint": bytes = BitConverter.GetBytes(Int64.Parse(curr_value)); break;
                                case "text": bytes = Encoding.Unicode.GetBytes(curr_value); break;
                                case "double": bytes = BitConverter.GetBytes(double.Parse(curr_value)); break;
                            }

                        byte[] nullflag = new byte[1];
                        if (curr_value == null)
                        {
                            nullflag[0] = 1;
                        }
                        else
                            nullflag[0] = 0;

                        st.Write(nullflag, 0, 1);

                        if (curr_value == null)
                        {
                            st.Write(nullflag, 0, 1); // rastgele değer
                        }
                        else
                        {
                            if (curr_value != null && list_spreaded_select_columns[i].type == "text") // string ise stringin uzunluğunu belirten header yaz
                            {
                                byte[] header = BitConverter.GetBytes(size);
                                st.Write(header, 0, sizeof(int));
                            }

                            st.Write(bytes, 0, size);
                        }
                    }

                    Int64 silinecekpos = Int64.Parse(whole_records[0][w][list_spreaded_select_columns.Count].ToString());
                    st.Seek(silinecekpos, SeekOrigin.Begin);

                    byte[] deletedflag2 = new byte[1];
                    deletedflag2[0] = 1;
                    st.Write(deletedflag2, 0, 1);
                }

                st.Close();

                for (var w = 0; w < whole_records[0].Count; ++w)
                {
                    for (int i = 0; i < curr_index.Count; ++i)
                    {
                        int gost = 0;

                        for (int k = 0; k < curr_index[i].sub_indexes.Count; ++k)
                        {
                            String index_key = list_afterkeyslist[w][i][gost++];
                            String recno2 = whole_records[0][w][list_spreaded_select_columns.Count].ToString();

                            var sql = "";
                            if (index_key == "\0")
                                sql = "UPDATE keyvalue SET value = " + filelens[w] + " WHERE key ISNULL AND value = "+ recno2;
                            else
                                sql = "UPDATE keyvalue SET value = " + filelens[w] + " WHERE key = '" + index_key.Replace("'", "''") + "' AND value = "+ recno2;

                            
                            
                            var cmd = new SQLiteCommand(sql, curr_index[i].sub_indexes[k].index_file);
                            cmd.ExecuteNonQuery();
                            cmd.Dispose();
                            
                            
                        }
                    }
                }

 
                logfp.Write(end_update_bytes, 0, end_update_bytes.Count());
                logfp.Close();
                
                return whole_records[0].Count.ToString();
            }
            else
                return "0";
        }

        List<List<Object>> set_select_error(String errstr) {
            var l_ret = new List<List<Object>>();
            var err = new List<Object>();
            err.Add(errstr);
            l_ret.Add(err);
            return l_ret;
        }

        String f_insert_into(String db, XmlDocument pdoc)
        {
            String tablename = pdoc.SelectNodes("//root/insertinto/table_name")[0].InnerText;
            XmlNodeList clist = pdoc.SelectNodes("//root/insertinto/col");
            XmlNodeList vlist = pdoc.SelectNodes("//root/values/val");

            if (!File.Exists("Databases/" + db + "/" + tablename + ".ssf"))
                return "Table does not exist!";

            if (clist.Count == 0)
                return "Columns must be defined!";
            if (vlist.Count == 0)
                return "Values must be defined!";
            if (clist.Count != vlist.Count)
                return "Columns count and Values count must be equal!";

            mytable curr_table = gettable(db, tablename);
            var columns_info = curr_table.columns_info;
            if (columns_info == null)
                return "Cannot get the table!";

            var begin_insert = "B";
            var end_insert = "E";

            byte[] begin_insert_bytes = Encoding.ASCII.GetBytes(begin_insert);
            byte[] end_insert_bytes = Encoding.ASCII.GetBytes(end_insert);

            for (var i = 0; i < columns_info.Count; ++i) {
                AlanInfo curr_column = columns_info[i];
                if (curr_column.ai == true) {
                    for (var k = 0; k < clist.Count; ++k)
                        if (clist[k].InnerText == curr_column.name)
                            return "The column named " + clist[k].InnerText + " is IDENTITY column, and it can't be defined in Columns list!";
                }
            }

            Int64 last_ai_value = 0;

            

            var ai_values = new List<Int64>();

            var insertion_values = new List<myval>();
            for (var i = 0; i < columns_info.Count; ++i)
            {
                myval curr_val = new myval();
                bool bulundu = false;
                int bulunanindis = -1;
                for (var k = 0; k < clist.Count; ++k)
                    if (clist[k].InnerText == columns_info[i].name)
                    {
                        bulundu = true;
                        bulunanindis = k;
                        break;
                    }

                if (bulundu == false)
                {
                    if (columns_info[i].ai == false)
                    {
                        curr_val.mynull = 1;
                        curr_val.type = 0;
                        insertion_values.Add(curr_val);
                        curr_val = new myval();
                        curr_val.mynull = curr_val.type = 0;
                        insertion_values.Add(curr_val);
                    }
                    else {  // autoincrement alan
                        String scurr_ai_value = File.ReadAllText("Databases/" + db + "/" + tablename + "." + columns_info[i].name +".inc");
                        long curr_ai_value = Convert.ToInt64(scurr_ai_value) + 1;
                        last_ai_value = curr_ai_value;
                        ai_values.Add(curr_ai_value);

                        curr_val.mynull = 0;
                        curr_val.type = 0;
                        insertion_values.Add(curr_val);
                        curr_val = new myval();
                        
                        switch (columns_info[i].type)
                        {
                            case "tinyint": curr_val.type = 1; curr_val.mychar = (sbyte) curr_ai_value; break;
                            case "smallint": curr_val.type = 2; curr_val.myint16 = Convert.ToInt16(curr_ai_value); break;
                            case "int": curr_val.type = 3; curr_val.myint32 = Convert.ToInt32(curr_ai_value); break;
                            case "bigint": curr_val.type = 4; curr_val.myint64 = Convert.ToInt64(curr_ai_value); break;
                            case "text": curr_val.type = 5; curr_val.mystring = Convert.ToString(curr_ai_value); break;
                            case "double": curr_val.type = 6; curr_val.mydouble = Convert.ToDouble(curr_ai_value); break;
                        }

                        insertion_values.Add(curr_val);
                    }
                }
                else {
                    if (vlist[bulunanindis].InnerText == "NULL")
                    {
                        curr_val.mynull = 1;
                        curr_val.type = 0;
                        insertion_values.Add(curr_val);
                        curr_val = new myval();
                        curr_val.mynull = curr_val.type = 0;
                        insertion_values.Add(curr_val);
                    }
                    else
                    {
                        switch (columns_info[i].type)
                        {
                            case "tinyint": 
                                sbyte ti;
                                if (sbyte.TryParse(vlist[bulunanindis].InnerText, out ti) == false)
                                {
                                    
                                    return (bulunanindis + 1) + ". value is bad format!";
                                }
                                break;
                            case "smallint":
                                Int16 si;
                                if (Int16.TryParse(vlist[bulunanindis].InnerText, out si) == false)
                                {
                                    
                                    return (bulunanindis + 1) + ". value is bad format!";
                                }
                                break;
                            case "int":
                                Int32 ii;
                                if (Int32.TryParse(vlist[bulunanindis].InnerText, out ii) == false)
                                {
                                    
                                    return (bulunanindis + 1) + ". value is bad format!";
                                }
                                break;
                            case "bigint":
                                Int64 bi;
                                if (Int64.TryParse(vlist[bulunanindis].InnerText, out bi) == false)
                                {
                                    
                                    return (bulunanindis + 1) + ". value is bad format!";
                                }
                                break;
                            case "double":
                                double dbl;
                                if (double.TryParse(vlist[bulunanindis].InnerText, out dbl) == false)
                                {
                                    
                                    return (bulunanindis + 1) + ". value is bad format!";
                                }
                                break;
                        }

                        switch (columns_info[i].type)
                        {
                            case "tinyint": curr_val.type = 1; curr_val.mychar = (sbyte) Convert.ToInt16(vlist[bulunanindis].InnerText); break;
                            case "smallint": curr_val.type = 2; curr_val.myint16 = Convert.ToInt16(vlist[bulunanindis].InnerText); break;
                            case "int": curr_val.type = 3; curr_val.myint32 = Convert.ToInt32(vlist[bulunanindis].InnerText); break;
                            case "bigint": curr_val.type = 4; curr_val.myint64 = Convert.ToInt64(vlist[bulunanindis].InnerText); break;
                            case "text": curr_val.type = 5; curr_val.mystring = Convert.ToString(vlist[bulunanindis].InnerText); break;
                            case "double": curr_val.type = 6; curr_val.mydouble = Convert.ToDouble(vlist[bulunanindis].InnerText); break;
                        }
                        
                        myval curr_val0 = new myval();
                        curr_val0.mynull = 0;
                        curr_val0.type = 0;
                        insertion_values.Add(curr_val0);
                        insertion_values.Add(curr_val);
                    }

                }

            }


            Stream st = File.Open("Databases/" + db + "/" + tablename + ".ssd", FileMode.Open, FileAccess.Write, FileShare.None);

            st.Seek(0, SeekOrigin.End);
            long filelen = st.Position;
            byte[] filelen_bytes = BitConverter.GetBytes(filelen);


            // unique alanların index dosyasıyla duplicate kontrolleri yapılıyor
            var curr_index = getindexes(db, curr_table.table_name);
            
            var afterkeyslist = new List<List<String>>();

            for (int i = 0; i < curr_index.Count; ++i) {
                var curr_subindexes = curr_index[i].sub_indexes;
                var duplicate_count = 0;
                var afterkeys = new List<String>();

                for (var k = 0; k < curr_subindexes.Count; ++k)
                {
                    var curr_index_file = curr_subindexes[k].index_file;
                       
                    var bulunanindis = -1;
                        
                    for (var j = 0; j < columns_info.Count; ++j)
                        if (columns_info[j].name == curr_subindexes[k].column_name) {
                            bulunanindis = j;
                            break;
                        }

                    var curr_value = "\0";
                    if (insertion_values[bulunanindis * 2].mynull != 1) {
                        switch (insertion_values[bulunanindis * 2 + 1].type) {
                            case 1: curr_value = Convert.ToString(insertion_values[bulunanindis * 2 + 1].mychar); break;
                            case 2: curr_value = Convert.ToString(insertion_values[bulunanindis * 2 + 1].myint16); break;
                            case 3: curr_value = Convert.ToString(insertion_values[bulunanindis * 2 + 1].myint32); break;
                            case 4: curr_value = Convert.ToString(insertion_values[bulunanindis * 2 + 1].myint64); break;
                            case 5: curr_value = Convert.ToString(insertion_values[bulunanindis * 2 + 1].mystring); break;
                            case 6: curr_value = Convert.ToString(insertion_values[bulunanindis * 2 + 1].mydouble); break;
                        }
                    }

                    if (curr_value == "NULL")
                        curr_value = "\0";
                        
                    afterkeys.Add(curr_value);

                    if (curr_index[i].unique == true)
                    {
                        String index_key = curr_value;
                        var sql = "";
                        if (index_key == "\0")
                            sql = "SELECT * FROM keyvalue WHERE key ISNULL";
                        else
                            sql = "SELECT * FROM keyvalue WHERE key = '" + index_key.Replace("'", "''") + "'";

                        

                        var cmd = new SQLiteCommand(sql, curr_index_file);
                        SQLiteDataReader r = cmd.ExecuteReader();

                        if (r.Read())
                        {
                            duplicate_count++;
                        }
                        r.Close();
                        cmd.Dispose();

                        
                    }
                }

                if (curr_index[i].unique == true && duplicate_count > 0 && duplicate_count == curr_subindexes.Count)
                {
                    st.Close();
                    
                    return "The index named ´" + curr_index[i].index_name + "´ does not allow duplicate entry!";
                }
                else { 
                    afterkeyslist.Add(afterkeys);
                }
            }

            Stream logfp = File.Open(curr_table.insert_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
            logfp.Seek(0, SeekOrigin.End);
            logfp.Write(begin_insert_bytes, 0, begin_insert_bytes.Count());
            logfp.Write(filelen_bytes, 0, filelen_bytes.Count());

            // // index dosyası veri girişi 
            for (int i = 0; i < curr_index.Count; ++i)
            {
                int gost = 0;

                for (int k = 0; k < curr_index[i].sub_indexes.Count; ++k)
                {
                    String index_key = afterkeyslist[i][gost++];
                    
                    var sql = "";
                    if (index_key == "\0")
                        sql = "INSERT INTO keyvalue VALUES (NULL, " + filelen + ")";
                    else
                        sql = "INSERT INTO keyvalue VALUES ('" + index_key.Replace("'", "''") + "', " + filelen + ")";
                    
                    
                    
                    var cmd = new SQLiteCommand(sql, curr_index[i].sub_indexes[k].index_file);
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    
                    
                }
            }

            byte[] deletedflag = new byte[1];
            deletedflag[0] = 0;
            st.Write(deletedflag, 0, 1);
            
            for (var i = 0; i < insertion_values.Count; ++i) {
                int size = 0;

                switch (insertion_values[i].type)
                {
                    case 0: size = sizeof(sbyte); break;
                    case 1: size = sizeof(sbyte); break;
                    case 2: size = sizeof(Int16); break;
                    case 3: size = sizeof(Int32); break;
                    case 4: size = sizeof(Int64); break;
                    case 5: size = Encoding.Unicode.GetBytes(insertion_values[i].mystring).Count(); break;
                    case 6: size = sizeof(double); break;
                }

                var tektirnakvar = false;

                if (insertion_values[i].type == 5 && insertion_values[i].mystring.Length > 0 && insertion_values[i].mystring.Substring(0, 1) == "'") 
                {
                    tektirnakvar = true;
                    size -= 2;
                    if (insertion_values[i].mystring.Length > 1 && insertion_values[i].mystring.Substring(insertion_values[i].mystring.Length - 1, 1) == "'")
                        size -= 2;
                }

                byte[] bytes = new byte[size];

                switch (insertion_values[i].type)
                {
                    case 0: bytes[0] = (byte) insertion_values[i].mynull; break;
                    case 1: bytes[0] = (byte) insertion_values[i].mychar; break;
                    case 2: bytes = BitConverter.GetBytes(insertion_values[i].myint16); break;
                    case 3: bytes = BitConverter.GetBytes(insertion_values[i].myint32); break;
                    case 4: bytes = BitConverter.GetBytes(insertion_values[i].myint64); break;
                    case 5: bytes = Encoding.Unicode.GetBytes(insertion_values[i].mystring); break;
                    case 6: bytes = BitConverter.GetBytes(insertion_values[i].mydouble); break;
                }

                if (insertion_values[i].type == 5) // string ise stringin uzunluğunu belirten header yaz
                {
                    byte[] header = BitConverter.GetBytes(size);
                    st.Write(header, 0, sizeof(int));
                }

                if (insertion_values[i].type == 5 && tektirnakvar == true)
                    st.Write(bytes, 2, size);
                else
                    st.Write(bytes, 0, size);
            }

            st.Close();

            int g = 0;
            for (var i = 0; i < columns_info.Count; ++i)
            {
                if (columns_info[i].ai == true)
                    File.WriteAllText("Databases/" + db + "/" + tablename + "." + columns_info[i].name + ".inc", ai_values[g++].ToString());
            }

            logfp.Write(end_insert_bytes, 0, end_insert_bytes.Count());
            logfp.Close();
            

            //MessageBox.Show(sizes);
            return last_ai_value.ToString();
        }

        List<AlanInfo> u_alan_info(String db, String tablename)
        {
            String info = File.ReadAllText("Databases/" + db + "/" + tablename + ".ssf");
            String[] pp = info.Split(new String[] {"\r\n-----\r\n"}, StringSplitOptions.None);

            String[] p_info = pp[0].Split(new String[] { "\r\n" }, StringSplitOptions.None);
            List<AlanInfo> l_alaninfo = new List<AlanInfo>();

            for (int i = 0; i < p_info.Length; ++i)
            {
                AlanInfo inf = new AlanInfo();
                String[] p = p_info[i].Split(' ');
                inf.name = p[0];
                inf.type = p[1];
                inf.ai = p.Length == 3 && p[2] == "IDENTITY" ? true : false;
                inf.ci = p.Length == 3 && p[2] == "CI" ? true : false;
                l_alaninfo.Add(inf);
            }

            return l_alaninfo;
        }

        String f_delete(String db, XmlDocument pdoc)
        {
            var l_ret = new List<List<Object>>();

            String tablename = pdoc.SelectNodes("//root/delete/table_name")[0].InnerText;
            XmlNodeList join_tablelist = pdoc.SelectNodes("//root/join/table_name");
            XmlNodeList join_onlist = pdoc.SelectNodes("//root/join/on");
            XmlNodeList join_typelist = pdoc.SelectNodes("//root/join/type");
            XmlNodeList whereexpression_list = pdoc.SelectNodes("//root/where/expression");
            XmlNodeList whereewithindex_list = pdoc.SelectNodes("//root/where/withindex");
            XmlNodeList orderby_list = pdoc.SelectNodes("//root/orderby");
            XmlNodeList orderbytype_list = pdoc.SelectNodes("//root/orderbytype");
            XmlNodeList groupby_list = pdoc.SelectNodes("//root/groupby");
            XmlNodeList limit_list = pdoc.SelectNodes("//root/limit");


            if (whereexpression_list.Count > 2)
                return "Select command can't contain more than 2 where clauses!";


            if (whereexpression_list.Count == 2)
            {
                if (whereewithindex_list[0].InnerText == "True" && whereewithindex_list[1].InnerText == "True" || whereewithindex_list[0].InnerText == "False" && whereewithindex_list[1].InnerText == "False")
                {

                    return "Two where clauses can't be same type!";

                }
                else if (whereewithindex_list[1].InnerText == "True")
                {

                    return "Second where clause can't be its withindex parameter is True!";

                }
            }
            if (groupby_list.Count > 0)
            {

                return ("Update command can't contain a groupby clause!");

            }
            if (limit_list.Count > 1)
            {

                return ("Update command can't contain a limit clause!");

            }
            if (join_tablelist.Count > 0)
            {

                return ("Update command can't contain a join clause!");

            }



            var list_select_columns = new List<select_column_struct>();

            var list_tables = new List<mytable>();

            var firsttable = gettable(db, tablename);

            if (firsttable == null) return (tablename + " doesn't exist!");

            list_tables.Add(firsttable);


            var list_spreaded_select_columns = new List<select_column_struct>();
            for (var k = 0; k < list_tables.Count; ++k)
            {
                for (var j = 0; j < list_tables[k].columns_info.Count; ++j)
                {
                    select_column_struct spreaded_column = new select_column_struct();
                    spreaded_column.ai = list_tables[k].columns_info[j].ai;
                    spreaded_column.ci = list_tables[k].columns_info[j].ci;
                    spreaded_column.name = list_tables[k].columns_info[j].name;
                    spreaded_column.type = list_tables[k].columns_info[j].type;
                    spreaded_column.table = list_tables[k];
                    var curr_indexes = getindexes(db, list_tables[k].table_name);
                    for (var g = 0; g < curr_indexes.Count; ++g)
                        for (var s = 0; s < curr_indexes[g].sub_indexes.Count; ++s)
                            if (spreaded_column.name == curr_indexes[g].sub_indexes[s].column_name)
                            {
                                spreaded_column.index = curr_indexes[g].sub_indexes[s];
                                goto CIK;
                            }
                CIK:
                    list_spreaded_select_columns.Add(spreaded_column);
                }
            }

            // kolonlar duplicate mi?
            for (var i = 0; i < list_spreaded_select_columns.Count; ++i)
            {

                String curr_name = list_spreaded_select_columns[i].alias;
                if (curr_name == null || curr_name == "") curr_name = list_spreaded_select_columns[i].name;

                for (var k = i + 1; k < list_spreaded_select_columns.Count; ++k)
                {
                    String curr_name2 = list_spreaded_select_columns[k].alias;
                    if (curr_name2 == null || curr_name2 == "") curr_name2 = list_spreaded_select_columns[k].name;

                    if (curr_name == curr_name2)
                        return ("The column named `" + curr_name + "` is ambigious!");
                }
            }

            // ilk where deyimi indexle mi alakalı?
            var kv_index_queried_columns = new List<select_column_struct>();

            if (whereexpression_list.Count > 0 && whereewithindex_list[0].InnerText == "True")
            {
                String indexed_where_exp = whereexpression_list[0].InnerText;
                if (indexed_where_exp == "") return ("First where clause is empty!");

                var testor = Regex.Split(indexed_where_exp, "\\|\\|(?=(?:[^']*'[^']*')*[^']*$)");
                if (testor.Length > 1) return ("|| operator is unsopperted in indexed where() function!");

                var parts = Regex.Split(indexed_where_exp, "&&(?=(?:[^']*'[^']*')*[^']*$)");

                for (var i = 0; i < parts.Count(); ++i)
                {
                    var m = parts[i];
                    var curr_op = "";
                    var pp = Regex.Split(m, " not like (?=(?:[^']*'[^']*')*[^']*$)");

                    if (pp.Length < 2)
                    {
                        pp = Regex.Split(m, "==(?=(?:[^']*'[^']*')*[^']*$)");
                        if (pp.Length < 2)
                        {
                            pp = Regex.Split(m, " like (?=(?:[^']*'[^']*')*[^']*$)");
                            if (pp.Length < 2)
                            {
                                pp = Regex.Split(m, "!=(?=(?:[^']*'[^']*')*[^']*$)");
                                if (pp.Length < 2)
                                {
                                    pp = Regex.Split(m, ">=(?=(?:[^']*'[^']*')*[^']*$)");
                                    if (pp.Length < 2)
                                    {
                                        pp = Regex.Split(m, "<=(?=(?:[^']*'[^']*')*[^']*$)");
                                        if (pp.Length < 2)
                                        {
                                            pp = Regex.Split(m, ">(?=(?:[^']*'[^']*')*[^']*$)");
                                            if (pp.Length < 2)
                                            {
                                                pp = Regex.Split(m, "<(?=(?:[^']*'[^']*')*[^']*$)");
                                                if (pp.Length < 2)
                                                    return ("There is invalid operator at first where expression!");
                                                else
                                                    curr_op = "<";
                                            }
                                            else
                                                curr_op = ">";
                                        }
                                        else
                                            curr_op = "<=";
                                    }
                                    else
                                        curr_op = ">=";
                                }
                                else
                                    curr_op = "!=";
                            }
                            else
                                curr_op = "like";
                        }
                        else
                            curr_op = "=";
                    }
                    else
                        curr_op = "not like";

                    String curr_column = pp[0].Trim();
                    String curr_value = pp[1].Trim();

                    var founded = false;

                    for (var k = 0; k < list_tables.Count; ++k)
                    {
                        for (var j = 0; j < list_tables[k].columns_info.Count; ++j)
                        {
                            if (curr_column == list_tables[k].columns_info[j].name)
                            {
                                founded = true;
                                select_column_struct column = new select_column_struct();
                                column.ai = list_tables[k].columns_info[j].ai;
                                column.ci = list_tables[k].columns_info[j].ci;
                                column.name = list_tables[k].columns_info[j].name;
                                column.type = list_tables[k].columns_info[j].type;
                                column.table = list_tables[k];
                                column.value = curr_value;
                                column.op = curr_op;
                                column.index = null;
                                var curr_indexes = getindexes(db, list_tables[k].table_name);
                                for (var g = 0; g < curr_indexes.Count; ++g)
                                    for (var s = 0; s < curr_indexes[g].sub_indexes.Count; ++s)
                                        if (column.name == curr_indexes[g].sub_indexes[s].column_name)
                                        {
                                            column.index = curr_indexes[g].sub_indexes[s];
                                            goto CIK4;
                                        }
                            CIK4:
                                kv_index_queried_columns.Add(column);
                            }
                        }
                    }

                    if (!founded) return ("The column named ´" + curr_column + "´ was not founded in related table! Code:2");
                }
            }

            //dosya gostericisi pozisyonlarını index dosyalarından sorgula
            var list_cursor_positions = new List<List<Int64>>();

            for (var i = 0; i < kv_index_queried_columns.Count; ++i)
            {
                if (kv_index_queried_columns[i].index == null)
                    return (kv_index_queried_columns[i].name + " has no index!");

                var cursor_positions = new List<Int64>();

                

                String index_key = kv_index_queried_columns[i].value;
                String op = kv_index_queried_columns[i].op;
                var ci = kv_index_queried_columns[i].ci;

                var sql = "";
                if (index_key == "NULL")
                {
                    if (op == "=")
                        sql = "SELECT * FROM keyvalue WHERE [key] ISNULL";
                    else if (op == "!=")
                        sql = "SELECT * FROM keyvalue WHERE NOT ([key] ISNULL)";
                    else
                        sql = "SELECT * FROM keyvalue WHERE [key] " + op + " NULL";
                }
                else if (ci == true && (op == "like" || op == "not like"))
                    sql = "SELECT * FROM keyvalue WHERE TOUPPER([key]) " + op + " TOUPPER(" + index_key + ")";
                else
                    sql = "SELECT * FROM keyvalue WHERE [key] " + op + " " + index_key;


                var cmd = new SQLiteCommand(sql, kv_index_queried_columns[i].index.index_file);
                SQLiteDataReader r;
                try
                {
                    r = cmd.ExecuteReader();
                }
                catch (Exception e)
                {
                    cmd.Dispose();
                    
                    return e.Message;
                }

                while (r.Read())
                {
                    cursor_positions.Add((long)r["value"]);
                }

                r.Close();
                cmd.Dispose();
                
                

                list_cursor_positions.Add(cursor_positions);
            }

            //gösterici pozisyonlarının kesişimlerini al
            var list_cursor_positions_son = new List<List<Int64>>();

            for (var i = 0; i < kv_index_queried_columns.Count; ++i)
            {
                var curr_cursor_positions = list_cursor_positions[i];
                for (var k = 0; k < kv_index_queried_columns.Count; ++k)
                {
                    if (i == k)
                        continue;
                    if (kv_index_queried_columns[i].table.table_name == kv_index_queried_columns[k].table.table_name)
                    {
                        curr_cursor_positions = curr_cursor_positions.Intersect(list_cursor_positions[k]).ToList();
                    }
                }
                list_cursor_positions_son.Add(curr_cursor_positions);
            }

            var list_cursor_positions_son2 = list_cursor_positions_son;

            //tablolar için tanımlı olmayanlara pozisyon değeri olarak -1 ata
            var list_cursor_positions_son3 = new List<List<Int64>>();

            for (var i = 0; i < list_tables.Count; ++i)
            {
                var poses = new List<Int64>();
                var found = false;
                for (var k = 0; k < kv_index_queried_columns.Count; ++k)
                {
                    if (list_tables[i].table_name == kv_index_queried_columns[k].table.table_name)
                    {
                        found = true;
                        for (var j = 0; j < list_cursor_positions_son2[k].Count; ++j)
                            poses.Add(list_cursor_positions_son2[k][j]);
                    }
                }

                //if (!found) poses.Add(-1);
                list_cursor_positions_son3.Add(poses);
            }

            for (var i = 0; i < list_cursor_positions_son3.Count; ++i)
            {
                if (list_cursor_positions_son3[i].Count == 0)
                {
                    var curr_tablename = list_tables[i].table_name;
                    var found = false;

                    for (var k = 0; k < kv_index_queried_columns.Count; ++k)
                        if (curr_tablename == kv_index_queried_columns[k].table.table_name)
                        {
                            found = true;
                            break;
                        }

                    if (!found) list_cursor_positions_son3[i].Add(-1);
                }
            }

            for (var i = 0; i < list_cursor_positions_son3.Count; ++i)
                list_cursor_positions_son3[i] = list_cursor_positions_son3[i].Distinct().ToList();

            var whole_records = new List<List<List<Object>>>();
            var whole_records2 = new List<List<List<Object>>>();

            // dosya işlemleri
            int size = 0;
            List<Stream> fps = new List<Stream>();
            for (var i = 0; i < list_tables.Count; ++i)
            {
                
                Stream fp = File.Open(list_tables[i].data_file_path, FileMode.Open, FileAccess.Read, FileShare.Read);
                fps.Add(fp);
            }

            for (var i = 0; i < list_cursor_positions_son3.Count; ++i)
            {
                var curr_table_records = new List<List<Object>>();
                if (list_cursor_positions_son3[i].Count == 0)
                    continue;

                if (list_cursor_positions_son3[i][0] == -1) // tüm kayıtları getir
                {
                    while (true)
                    {
                        var curr_record = new List<Object>();
                        Int64 curr_pos = fps[i].Position;

                        var deleted_flag = new byte[1];
                        if (fps[i].Read(deleted_flag, 0, 1) == 0)
                            break;

                        if (deleted_flag[0] == 1)
                        {
                            for (var k = 0; k < list_tables[i].columns_info.Count; ++k)
                            {
                                var null_flag = new byte[1];
                                fps[i].Read(null_flag, 0, 1);
                                if (null_flag[0] == 1)
                                {
                                    fps[i].Seek(1, SeekOrigin.Current);
                                    continue;
                                }

                                switch (list_tables[i].columns_info[k].type)
                                {
                                    case "tinyint": size = sizeof(sbyte); break;
                                    case "smallint": size = sizeof(Int16); break;
                                    case "int": size = sizeof(Int32); break;
                                    case "bigint": size = sizeof(Int64); break;
                                    case "text":
                                        var bytesofsize = new byte[sizeof(int)];
                                        fps[i].Read(bytesofsize, 0, sizeof(int));
                                        size = BitConverter.ToInt32(bytesofsize, 0);
                                        break;
                                    case "double": size = sizeof(double); break;
                                }
                                fps[i].Seek(size, SeekOrigin.Current);
                            }
                            continue;
                        }

                        var curr_column_value = new Object();
                        // deleted flag = 0, verileri çek

                        for (var j = 0; j < list_tables[i].columns_info.Count; ++j)
                        {
                            var null_flag = new byte[1];
                            fps[i].Read(null_flag, 0, 1);
                            if (null_flag[0] == 1)
                            {
                                fps[i].Seek(1, SeekOrigin.Current);
                                curr_record.Add(null);
                                continue;
                            }

                            byte[] curr_column_bytes;

                            switch (list_tables[i].columns_info[j].type)
                            {
                                case "tinyint":
                                    size = sizeof(sbyte);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = curr_column_bytes[0];
                                    break;
                                case "smallint":
                                    size = sizeof(Int16);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt16(curr_column_bytes, 0);
                                    break;
                                case "int": size =
                                    sizeof(Int32);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt32(curr_column_bytes, 0);
                                    break;
                                case "bigint":
                                    size = sizeof(Int64);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt64(curr_column_bytes, 0);
                                    break;
                                case "text":
                                    var bytesofsize = new byte[sizeof(int)];
                                    fps[i].Read(bytesofsize, 0, sizeof(int));
                                    size = BitConverter.ToInt32(bytesofsize, 0);

                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);

                                    curr_column_value = Encoding.Unicode.GetString(curr_column_bytes, 0, size);
                                    break;
                                case "double":
                                    size = sizeof(double);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToDouble(curr_column_bytes, 0);
                                    break;

                            }

                            for (var k = 0; k < list_spreaded_select_columns.Count; ++k)
                            {
                                if (list_tables[i].columns_info[j].name == list_spreaded_select_columns[k].name)
                                {
                                    curr_record.Add(curr_column_value);
                                }
                            }
                        }

                        curr_record.Add(curr_pos);
                        curr_table_records.Add(curr_record);
                    }

                }
                else // indexten gelen kayıtları getir
                {
                    for (var g = 0; g < list_cursor_positions_son3[i].Count; ++g)
                    {
                        var curr_record = new List<Object>();

                        Int64 curr_pos = list_cursor_positions_son3[i][g];

                        fps[i].Seek(curr_pos, SeekOrigin.Begin);

                        var deleted_flag = new byte[1];
                        fps[i].Read(deleted_flag, 0, 1);

                        if (deleted_flag[0] == 1)
                            continue;

                        var curr_column_value = new Object();
                        // deleted flag = 0, verileri çek

                        for (var j = 0; j < list_tables[i].columns_info.Count; ++j)
                        {
                            var null_flag = new byte[1];
                            fps[i].Read(null_flag, 0, 1);
                            if (null_flag[0] == 1)
                            {
                                fps[i].Seek(1, SeekOrigin.Current);
                                curr_record.Add(null);
                                continue;
                            }

                            byte[] curr_column_bytes;

                            switch (list_tables[i].columns_info[j].type)
                            {
                                case "tinyint":
                                    size = sizeof(sbyte);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = curr_column_bytes[0];
                                    break;
                                case "smallint":
                                    size = sizeof(Int16);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt16(curr_column_bytes, 0);
                                    break;
                                case "int": size =
                                    sizeof(Int32);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt32(curr_column_bytes, 0);
                                    break;
                                case "bigint":
                                    size = sizeof(Int64);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToInt64(curr_column_bytes, 0);
                                    break;
                                case "text":
                                    var bytesofsize = new byte[sizeof(int)];
                                    fps[i].Read(bytesofsize, 0, sizeof(int));
                                    size = BitConverter.ToInt32(bytesofsize, 0);

                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);

                                    curr_column_value = Encoding.Unicode.GetString(curr_column_bytes, 0, size);
                                    break;
                                case "double":
                                    size = sizeof(double);
                                    curr_column_bytes = new byte[size];
                                    fps[i].Read(curr_column_bytes, 0, size);
                                    curr_column_value = BitConverter.ToDouble(curr_column_bytes, 0);
                                    break;

                            }

                            for (var k = 0; k < list_spreaded_select_columns.Count; ++k)
                            {
                                if (list_tables[i].columns_info[j].name == list_spreaded_select_columns[k].name)
                                {
                                    curr_record.Add(curr_column_value);
                                }
                            }
                        }

                        curr_record.Add(curr_pos);
                        curr_table_records.Add(curr_record);
                    }
                }

                whole_records.Add(curr_table_records);
            }

            for (var i = 0; i < list_tables.Count; ++i)
            {
                fps[i].Close();
                
            }


            //linqtosql sorguları
            var on_exp_list = new List<String>();
            for (var i = 0; i < join_onlist.Count; ++i)
            {
                var curr_on = join_onlist[i].InnerText;
                var parts = Regex.Split(curr_on, "==(?=(?:[^']*'[^']*')*[^']*$)");
                if (parts.Length < 2) return ((i + 1) + ". on expression must contain == operator!");
                var column1 = parts[0].Trim();
                var column2 = parts[1].Trim();

                var order1 = get_column_order(column1, ref list_spreaded_select_columns);
                var order2 = get_column_order(column2, ref list_spreaded_select_columns);

                var type = join_typelist[i].InnerText;

                if (type == "inner")
                {
                    column1 = order1.table_name + "_sl[" + order1.indis + "]";
                    column2 = order2.table_name + "_sl[" + order2.indis + "]";
                }
                else
                {
                    column1 = order1.table_name + "_sl[" + order1.indis + "]";
                    column2 = "lr" + (i + 1) + "_sl[" + order2.indis + "]";
                    if (list_tables[i + 1].table_name != order2.table_name)
                        return (i + 1) +". `on` parameter's right table's column name is invalid!";
                }
                if (order1.ci == true && order2.ci == true)
                {
                    column1 += ".ToString().ToLower()";
                    column2 += ".ToString().ToLower()";
                }
                on_exp_list.Add(column1 + " equals " + column2);
            }

            String where_exp_without_index = "";

            if (whereexpression_list.Count == 1 && whereewithindex_list[0].InnerText == "False")
            {
                where_exp_without_index = whereexpression_list[0].InnerText;
            }
            else if (whereexpression_list.Count == 2 && whereewithindex_list[1].InnerText == "False")
            {
                where_exp_without_index = whereexpression_list[1].InnerText;
            }

            if (list_tables.Count > 1 || where_exp_without_index != "" || groupby_list.Count > 0 || orderby_list.Count > 0)
            {

                var sql = "(\nfrom " + list_tables[0].table_name + "_sl in tables[0]\n";
                for (var i = 1; i < list_tables.Count; ++i)
                {
                    if (join_typelist[i - 1].InnerText == "inner")
                        sql += "join " + list_tables[i].table_name + "_sl in tables[" + i + "] on " + on_exp_list[i - 1] + "\n";
                    else
                    {
                        sql += "join lr" + i + "_sl in tables[" + i + "] on " + on_exp_list[i - 1] + " into lrs" + i + "\n";
                        sql += "from " + list_tables[i].table_name + "_sl in lrs" + i + ".DefaultIfEmpty()\n";
                    }
                }
                // where exp çözümle

                if (where_exp_without_index != "")
                    if (where_exp_without_index != "")
                    {
                        where_exp_without_index = Regex.Replace(where_exp_without_index, "&&(?=(?:[^']*'[^']*')*[^']*$)", "&");
                        where_exp_without_index = Regex.Replace(where_exp_without_index, "\\|\\|(?=(?:[^']*'[^']*')*[^']*$)", "|");

                        var mc = Regex.Split(where_exp_without_index, "[&|](?=(?:[^']*'[^']*')*[^']*$)");

                        var parts = new List<String>();

                        foreach (String m in mc)
                        {
                            var sm = Regex.Replace(m, "\\((?=(?:[^']*'[^']*')*[^']*$)", "");
                            sm = Regex.Replace(sm, "\\)(?=(?:[^']*'[^']*')*[^']*$)", "");
                            sm = sm.Trim();
                            parts.Add(sm);
                        }

                        parts = (from s in parts
                                 orderby s.Length descending
                                 select s).ToList();
                        var parts2 = new List<String>();

                        for (var i = 0; i < parts.Count; ++i)
                        {
                            var oper = "not like";
                            var p = Regex.Split(parts[i], " not like (?=(?:[^']*'[^']*')*[^']*$)");
                            if (p.Length == 1)
                            {
                                p = Regex.Split(parts[i], " like (?=(?:[^']*'[^']*')*[^']*$)");
                                if (p.Length > 1) oper = "like";
                            }
                            if (p.Length > 1)
                            {
                                p[0] = p[0].Trim();
                                p[1] = p[1].Trim();

                                if (p[0] == "" || p[1] == "")
                                    return "Statement was bad format!";

                                var p0isvalue = false;
                                var p1isvalue = false;

                                if (p[0].Substring(0, 1) == "'")
                                {
                                    p0isvalue = true;
                                    p[0] = p[0].Substring(1);
                                    if (p[0].Length > 0 && p[0].Substring(p[0].Length - 1, 1) == "'")
                                        p[0] = p[0].Substring(0, p[0].Length - 1);
                                }
                                else if (p[0] == "NULL")
                                    p[0] = null;

                                if (p[1].Substring(0, 1) == "'")
                                {
                                    p1isvalue = true;
                                    p[1] = p[1].Substring(1);
                                    if (p[1].Length > 0 && p[1].Substring(p[1].Length - 1, 1) == "'")
                                        p[1] = p[1].Substring(0, p[1].Length - 1);
                                }
                                else if (p[1] == "NULL")
                                    p[1] = null;


                                column_order_struct o = get_column_order(p[0], ref list_spreaded_select_columns);
                                column_order_struct o2 = get_column_order(p[1], ref list_spreaded_select_columns);

                                if (p0isvalue) o.table_name = null;
                                if (p1isvalue) o2.table_name = null;

                                var cond = oper == "like" ? "true" : "false";

                                var ekcik = "";
                                var ekciu = "";
                                var ekcik0 = "";
                                var ekciu0 = "";
                                var ekcik1 = "";
                                var ekciu1 = "";

                                if ((o.ci == true && o2.ci == true) || (o.ci == true && p1isvalue) || (p0isvalue && o2.ci == true))
                                {
                                    ekcik = ".ToLower()";
                                    ekciu = ".ToString().ToLower()";
                                    ekcik0 = ".ToLower()";
                                    ekciu0 = ".ToString().ToLower()";
                                    ekcik1 = ".ToLower()";
                                    ekciu1 = ".ToString().ToLower()";

                                    if (p[0] == null)
                                    {
                                        ekcik0 = "";
                                        ekciu0 = "";
                                    }
                                    if (p[1] == null)
                                    {
                                        ekcik1 = "";
                                        ekciu1 = "";
                                    }
                                }

                                String part = "";
                                if (o.table_name != null && o2.table_name != null)
                                    part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "] == null) ? \"\" : " + o2.table_name + "_sl[" + o2.indis + "].ToString()" + ekcik + "), ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "] == null) ? \"\" : " + o.table_name + "_sl[" + o.indis + "].ToString()" + ekcik + ")) == " + cond;
                                else if (o.table_name == null && o2.table_name == null)
                                    part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(\"" + p[1] + "\"" + ekciu1 + ", ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(\"" + p[0] + "\"" + ekciu0 + ")) == " + cond;
                                else if (o.table_name == null)
                                    part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "] == null) ? \"\" : " + o2.table_name + "_sl[" + o2.indis + "].ToString()" + ekcik + "), ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(\"" + p[0] + "\"" + ekciu0 + ") == " + cond;
                                else
                                    part = "new System.Text.RegularExpressions.Regex(@\"\\A\" + new System.Text.RegularExpressions.Regex(@\"\\.|\\$|\\{|\\(|\\||\\)|\\*|\\+|\\?|\\\\\").Replace(\"" + p[1] + "\"" + ekciu1 + ", ch => @\"\\\" + ch).Replace('_', '.').Replace(\"%\", \".*\") + @\"\\z\", System.Text.RegularExpressions.RegexOptions.Singleline).IsMatch(((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "] == null) ? \"\" : " + o.table_name + "_sl[" + o.indis + "].ToString()" + ekcik + ")) == " + cond;

                                parts2.Add(part);
                            }
                            else
                            {
                                String op = "";

                                p = Regex.Split(parts[i], "==(?=(?:[^']*'[^']*')*[^']*$)");
                                if (p.Length == 2)
                                    op = "==";
                                else
                                {
                                    p = Regex.Split(parts[i], "!=(?=(?:[^']*'[^']*')*[^']*$)");
                                    if (p.Length == 2)
                                        op = "!=";
                                    else
                                    {
                                        p = Regex.Split(parts[i], ">=(?=(?:[^']*'[^']*')*[^']*$)");
                                        if (p.Length == 2)
                                            op = ">=";
                                        else
                                        {
                                            p = Regex.Split(parts[i], "<=(?=(?:[^']*'[^']*')*[^']*$)");
                                            if (p.Length == 2)
                                                op = "<=";
                                            else
                                            {
                                                p = Regex.Split(parts[i], ">(?=(?:[^']*'[^']*')*[^']*$)");
                                                if (p.Length == 2)
                                                    op = ">";
                                                else
                                                {
                                                    p = Regex.Split(parts[i], "<(?=(?:[^']*'[^']*')*[^']*$)");
                                                    if (p.Length == 2)
                                                        op = "<";
                                                    else
                                                    {
                                                        return ("Invalid operator(s) in where clause!");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                p[0] = p[0].Trim();
                                p[1] = p[1].Trim();

                                if (p[0] == "" || p[1] == "")
                                    return "Statement was bad format!";

                                var p0isvalue = false;
                                var p1isvalue = false;

                                if (p[0].Substring(0, 1) == "'")
                                {
                                    p0isvalue = true;
                                    p[0] = p[0].Substring(1);
                                    if (p[0].Length > 0 && p[0].Substring(p[0].Length - 1, 1) == "'")
                                        p[0] = p[0].Substring(0, p[0].Length - 1);
                                }
                                else if (p[0] == "NULL")
                                    p[0] = null;

                                if (p[1].Substring(0, 1) == "'")
                                {
                                    p1isvalue = true;
                                    p[1] = p[1].Substring(1);
                                    p[1] = p[1].Substring(0, p[1].Length - 1);
                                }
                                else if (p[1] == "NULL")
                                    p[1] = null;

                                column_order_struct o = get_column_order(p[0], ref list_spreaded_select_columns);
                                column_order_struct o2 = get_column_order(p[1], ref list_spreaded_select_columns);

                                var ekcik = "";
                                var ekciu = "";
                                var ekcik0 = "";
                                var ekciu0 = "";
                                var ekcik1 = "";
                                var ekciu1 = "";

                                if ((o.ci == true && o2.ci == true) || (o.ci == true && p1isvalue) || (p0isvalue && o2.ci == true))
                                {
                                    ekcik = ".ToLower()";
                                    ekciu = ".ToString().ToLower()";
                                    ekcik0 = ".ToLower()";
                                    ekciu0 = ".ToString().ToLower()";
                                    ekcik1 = ".ToLower()";
                                    ekciu1 = ".ToString().ToLower()";

                                    if (p[0] == null)
                                    {
                                        ekcik0 = "";
                                        ekciu0 = "";
                                    }
                                    if (p[1] == null)
                                    {
                                        ekcik1 = "";
                                        ekciu1 = "";
                                    }
                                }

                                String part = "";
                                if (o.table_name != null && o2.table_name != null)
                                {
                                    var type1 = list_spreaded_select_columns[o.indis2].type;
                                    var type2 = list_spreaded_select_columns[o2.indis2].type;
                                    if (type1 == "text" || type2 == "text")
                                        part = "string.Compare(((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "]==null) ? null : " + o.table_name + "_sl[" + o.indis + "].ToString()" + ekcik + "), ((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "]==null) ? null : " + o2.table_name + "_sl[" + o2.indis + "].ToString()" + ekcik + ")) " + op + " 0";
                                    else
                                    {
                                        part = "((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "]==null) ? 0 : double.Parse(" + o.table_name + "_sl[" + o.indis + "].ToString())) ";
                                        part += op + " ";
                                        part += "((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "]==null) ? 0 : double.Parse(" + o2.table_name + "_sl[" + o2.indis + "].ToString()))";
                                    }
                                }
                                else if (o.table_name == null && o2.table_name == null)
                                {
                                    double res = 0;
                                    Int64 res2;

                                    var type1 = double.TryParse(p[0], out res) || Int64.TryParse(p[0], out res2) ? "" : "text";
                                    var type2 = double.TryParse(p[1], out res) || Int64.TryParse(p[1], out res2) ? "" : "text";

                                    if (type1 == "text" || type2 == "text")
                                        part = "string.Compare(" + (p[0] == null ? "null" : "\"" + p[0] + "\"" + ekciu0 + "") + ", " + (p[1] == null ? "null" : "\"" + p[1] + "\"" + ekciu1 + "") + ") " + op + " 0";
                                    else
                                    {
                                        part = p[0] + " ";
                                        part += op + " ";
                                        part += p[1];
                                    }

                                }
                                else if (o.table_name == null)
                                {
                                    double res = 0;
                                    Int64 res2;

                                    var type1 = double.TryParse(p[0], out res) || Int64.TryParse(p[0], out res2) ? "" : "text";
                                    var type2 = list_spreaded_select_columns[o2.indis2].type;

                                    if (type1 == "text" || type2 == "text")
                                        part = "string.Compare(" + (p[0] == null ? "null" : "\"" + p[0] + "\"" + ekciu0 + "") + ", ((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "]==null) ? null : " + o2.table_name + "_sl[" + o2.indis + "].ToString()" + ekcik + ")) " + op + " 0";
                                    else
                                    {
                                        part = p[0] + " ";
                                        part += op + " ";
                                        part += "((" + o2.table_name + "_sl==null || " + o2.table_name + "_sl[" + o2.indis + "]==null) ? 0 : double.Parse(" + o2.table_name + "_sl[" + o2.indis + "].ToString()))";
                                    }

                                }
                                else if (o2.table_name == null)
                                {
                                    double res = 0;
                                    Int64 res2 = 0;

                                    var type1 = list_spreaded_select_columns[o.indis2].type;
                                    var type2 = double.TryParse(p[1], out res) || Int64.TryParse(p[1], out res2) ? "" : "text";

                                    if (type1 == "text" || type2 == "text")
                                        part = "string.Compare(((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "]==null) ? null : " + o.table_name + "_sl[" + o.indis + "].ToString()" + ekcik + "), " + (p[1] == null ? "null" : "\"" + p[1] + "\"" + ekciu1 + "") + ") " + op + " 0";
                                    else
                                    {
                                        part += "((" + o.table_name + "_sl==null || " + o.table_name + "_sl[" + o.indis + "]==null) ? 0 : double.Parse(" + o.table_name + "_sl[" + o.indis + "].ToString())) ";
                                        part += op + " ";
                                        part += p[1];
                                    }

                                }

                                parts2.Add(part);
                            }
                        }

                        where_exp_without_index = Regex.Replace(where_exp_without_index, "&(?=(?:[^']*'[^']*')*[^']*$)", "&&");
                        where_exp_without_index = Regex.Replace(where_exp_without_index, "\\|(?=(?:[^']*'[^']*')*[^']*$)", "||");

                        for (var i = 0; i < parts.Count; ++i)
                            where_exp_without_index = where_exp_without_index.Replace(parts[i], parts2[i]);

                        sql += "where " + where_exp_without_index + "\n";
                    }

                var new_str = "";

                for (var i = 0; i < list_spreaded_select_columns.Count; ++i)
                {
                    if (list_spreaded_select_columns[i].name == "*")
                        new_str += "null,";
                    else
                    {
                        var o = get_column_order(list_spreaded_select_columns[i].name, ref list_spreaded_select_columns);
                        new_str += "(" + o.table_name + "_sl==null?null:" + o.table_name + "_sl[" + o.indis + "]),";
                    }
                }

                new_str += list_spreaded_select_columns[0].table.table_name + "_sl[" + (list_spreaded_select_columns.Count) + "],";
                new_str = new_str.Substring(0, new_str.Length - 1);
                sql += "select new List<Object> {" + new_str + "}\n)";


                //orderby
                if (orderby_list.Count > 0)
                {
                    for (var i = 0; i < orderby_list.Count; ++i)
                    {
                        var oparts = orderby_list[i].InnerText.Split(',');
                        var asc_desc = orderbytype_list.Count > i ? orderbytype_list[i].InnerText : "asc";
                        column_order_struct o = get_column_order(oparts[0].Trim(), ref list_spreaded_select_columns);
                        if (o.table_name == null) return ("Invalid column named `" + oparts[0].Trim() + "` in order by clause!");

                        if (i == 0)
                        {
                            if (asc_desc == "asc")
                                sql += ".OrderBy(x => x[" + o.indis2 + "])";
                            else
                                sql += ".OrderByDescending(x => x[" + o.indis2 + "])";
                        }
                        else
                        {
                            if (asc_desc == "asc")
                                sql += ".ThenBy(x => x[" + o.indis2 + "])";
                            else
                                sql += ".ThenByDescending(x => x[" + o.indis2 + "])";
                        }

                        for (var k = 1; k < oparts.Length; ++k)
                        {
                            o = get_column_order(oparts[k].Trim(), ref list_spreaded_select_columns);
                            if (o.table_name == null) return ("Invalid column named `" + oparts[k].Trim() + "` in order by clause!");
                            if (asc_desc == "asc")
                                sql += ".ThenBy(x => x[" + o.indis2 + "])";
                            else
                                sql += ".ThenByDescending(x => x[" + o.indis2 + "])";
                        }
                    }

                    sql += "\n";
                }
                //File.WriteAllText("sql.txt", sql);
                EvalCSCode e = new EvalCSCode();

                try
                {
                    whole_records[0] = e.EvalWithParams(
                    "var culture_info = System.Globalization.CultureInfo.CreateSpecificCulture(c);\n" +
                    "culture_info.NumberFormat.NumberDecimalSeparator = \".\";\n" +
                    "System.Threading.Thread.CurrentThread.CurrentCulture = culture_info;\n" +
                    "return " + sql + ";\n",
                    "List<List<List<Object>>> tables, string c", whole_records, cultureinfostr).ToList();

                    //MessageBox.Show(sonuc.Count.ToString());
                }
                catch (Exception ex)
                {
                    //MessageBox.Show(ex.Message);
                    return ex.Message;
                }

            }

            whole_records2.Add(new List<List<Object>>());

            for (var i = 0; i < whole_records[0].Count; ++i)
            {
                var new_list = new List<Object>();
                new_list = whole_records[0][i];
                whole_records2[0].Add(new_list);
            }

            //dosya işlemleri
            if (whole_records.Count > 0)
            {
                try
                {
                    if (whole_records[0].Count == 0)
                        return "0";
                }
                catch (Exception ex) {
                    return "0";
                }


                //insert to file
                var begin_update = "B";
                var end_update = "E";

                byte[] begin_update_bytes = Encoding.ASCII.GetBytes(begin_update);
                byte[] end_update_bytes = Encoding.ASCII.GetBytes(end_update);

                mytable curr_table = gettable(db, list_tables[0].table_name);

                byte[] update_records_count_bytes = BitConverter.GetBytes((Int64) whole_records[0].Count);

                

                Stream logfp = File.Open(curr_table.delete_log_file_path, FileMode.Create, FileAccess.Write, FileShare.None);
                logfp.Seek(0, SeekOrigin.End);
                logfp.Write(begin_update_bytes, 0, begin_update_bytes.Count());
                logfp.Write(update_records_count_bytes, 0, update_records_count_bytes.Count());

                for (var i = 0; i < whole_records[0].Count; ++i)
                {
                    byte[] rec_bytes = BitConverter.GetBytes(Int64.Parse(whole_records[0][i][list_spreaded_select_columns.Count].ToString()));
                    logfp.Write(rec_bytes, 0, rec_bytes.Count());
                }

                for (var i = 0; i < list_spreaded_select_columns.Count; ++i)
                {
                    if (list_spreaded_select_columns[i].index != null)
                    {
                        
                        

                        var cmd = new SQLiteCommand();
                        cmd.Connection = list_spreaded_select_columns[i].index.index_file;

                        var rb = new rollback_struct();
                        rb.conn = list_spreaded_select_columns[i].index.index_file;
                        
                        for (var k = 0; k < whole_records[0].Count; ++k)
                        {
                            var sql = "DELETE FROM keyvalue WHERE value = " + whole_records[0][k][list_spreaded_select_columns.Count].ToString();
                            cmd.CommandText = sql;
                            cmd.ExecuteNonQuery();
                        }

                        cmd.Dispose();
                        
                        
                    }
                }

                var curr_index = getindexes(db, curr_table.table_name);


                Stream st = File.Open("Databases/" + db + "/" + tablename + ".ssd", FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                for (var w = 0; w < whole_records[0].Count; ++w)
                {
                    Int64 silinecekpos = Int64.Parse(whole_records[0][w][list_spreaded_select_columns.Count].ToString());
                    st.Seek(silinecekpos, SeekOrigin.Begin);

                    byte[] deletedflag2 = new byte[1];
                    deletedflag2[0] = 1;
                    st.Write(deletedflag2, 0, 1);
                }
                st.Close();

                logfp.Write(end_update_bytes, 0, end_update_bytes.Count());
                logfp.Close();
                
                return whole_records[0].Count.ToString();
            }
            else
                return "0";
        }
        String f_create_index(String db, XmlDocument pdoc)
        {
            String name = pdoc.SelectNodes("//root/createindex/name")[0].InnerText;
            
            if (File.Exists("Databases/" + db + "/" + name + ".indexdef")) {
                
                return "Index already exists!";
            }

            String tablename = pdoc.SelectNodes("//root/createindex/table_name")[0].InnerText;
            String unique = pdoc.SelectNodes("//root/createindex/unique")[0].InnerText;
            XmlNodeList clist = pdoc.SelectNodes("//root/createindex/col");

            if (!File.Exists("Databases/" + db + "/" + tablename + ".ssf"))
            {
                
                return "Table doesn't exist!";
            }

            FileInfo f = new FileInfo("Databases/" + db + "/" + tablename + ".ssd");
            if (f.Length > 0)
            {
                
                return "Data file size of table must be zero byte!";
            }

            File.WriteAllText("Databases/" + db + "/index_creation.log", tablename + " " + name + "\r\nB");

            for (int i = 0; i < clist.Count; ++i)
            {
                Boolean bulundu = false;
                String columnname = clist[i].InnerText;

                for (var k = 0; k < tables.Count; ++k)
                {
                    for (var j = 0; j < tables[k].columns_info.Count; ++j)
                    {
                        if (tables[k].database_name == db && tables[k].table_name == tablename && tables[k].columns_info[j].name == columnname)
                        {
                            bulundu = true;
                            goto CIK1;
                        }
                    }
                }
                CIK1:
                if (!bulundu)
                {
                    File.AppendAllText("Databases/" + db + "/index_creation.log", "\r\nE");
                    
                    return "Related table doesn't contain a column named " + columnname;
                }
            }

            var table_form_content = File.ReadAllText("Databases/" + db + "/" + tablename + ".ssf");
            if (table_form_content.IndexOf("-----") < 0)
                table_form_content += "\r\n-----";

            table_form_content += "\r\n" + name + (unique == "True" ? " UNIQUE" : "");
            File.WriteAllText("Databases/" + db + "/" + tablename + ".ssf", table_form_content);

            myindex my_index = new myindex();
            my_index.database_name = db;
            my_index.file_path = "Databases/" + db + "/" + name + ".indexdef";
            my_index.index_name = name;
            my_index.table_name = tablename;
            my_index.unique = unique == "True" ? true : false;

            File.WriteAllText("Databases/" + db + "/" + name + ".indexdef", "");

            for (int i = 0; i < clist.Count; ++i)
            {
                String filename2;
                    
                do
                {
                    filename2 = random_file_name();
                } while (File.Exists("Databases/" + db + "/" + filename2 + ".ndx"));
                    
                mysubindex my_subindex = new mysubindex();
                my_subindex.column_name = clist[i].InnerText;
                my_subindex.file_path = "Databases/" + db + "/" + filename2 +".ndx";
                var info = u_alan_info(db, tablename);
                AlanInfo inf = new AlanInfo();
                for (var ii = 0; ii < info.Count; ++ii)
                    if (my_subindex.column_name == info[ii].name)
                        inf = info[ii];

                switch (inf.type) {
                    case "text":
                        if (inf.ci)
                            File.Copy("index_template_text_ci.ndx", my_subindex.file_path, true);
                        else
                            File.Copy("index_template_text_cs.ndx", my_subindex.file_path, true);
                        break;
                    case "double": File.Copy("index_template_double.ndx", my_subindex.file_path, true); break;
                    case "tinyint":
                    case "smallint":
                    case "int":
                    case "bigint":
                        File.Copy("index_template_int.ndx", my_subindex.file_path, true); break;
                }
 
                my_subindex.id = filename2;
                my_subindex.index_file = new SQLiteConnection("Data Source=" + my_subindex.file_path);
                my_subindex.index_file.Open();

                my_index.sub_indexes.Add(my_subindex);

                if (i != 0)
                    File.AppendAllText("Databases/" + db + "/" + name + ".indexdef", "\r\n");

                File.AppendAllText("Databases/" + db + "/" + name + ".indexdef", my_subindex.column_name + " " + my_subindex.id);
            }

            
            indexes.Add(my_index);
            


            File.AppendAllText("Databases/" + db + "/index_creation.log", "\r\nE");
            

            return "OK";
        }

        String f_create_table(String db, XmlDocument pdoc)
        {
            String tablename = pdoc.SelectNodes("//root/createtable/table_name")[0].InnerText;

            XmlNodeList nlist = pdoc.SelectNodes("//root/createtable/col");

            int cnt = nlist.Count;

            
            File.WriteAllText("Databases/" + db + "/table_creation.log", tablename +"\r\nB");

            if (File.Exists("Databases/" + db + "/" + tablename + ".ssf"))
            {
                File.Delete("Databases/" + db + "/table_creation.log");
                
                return "Table already exist!";
            }
                
            String indexsection = "";
            int ilk = 1;
            List<myindex> myindexes = new List<myindex>();

            var identity_count = 0;
            var hatalitur = false;
            var hatalitur2 = false;

            for (int i = 0; i < cnt; ++i) {
                if (nlist[i].InnerText == "")
                {
                    File.Delete("Databases/" + db + "/table_creation.log");
                    
                    return "Column definition must not be emtpy!";
                }

                var p = nlist[i].InnerText.Split(' ');

                if (p.Length < 2 || p.Length > 3)
                {
                    File.Delete("Databases/" + db + "/table_creation.log");
                    
                    return "Column definition format is invalid!";
                }

                if (p[0] == "")
                {
                    File.Delete("Databases/" + db + "/table_creation.log");
                    
                    return "Column name is invalid!";
                }

                if (p[1] != "tinyint" && p[1] != "smallint" && p[1] != "int" && p[1] != "bigint" && p[1] != "text" && p[1] != "double")
                {
                    File.Delete("Databases/" + db + "/table_creation.log");
                    
                    return "Column type is invalid!";
                }

                if (p.Length == 3 && p[2] != "IDENTITY" && p[2] != "CI")
                {
                    File.Delete("Databases/" + db + "/table_creation.log");
                    
                    return "Column definition row's 3. identifier is invalid!";
                }

                if (p.Length == 3)
                {
                    if (p[2] == "IDENTITY")
                        ++identity_count;

                    if ((p[1] == "text" || p[1] == "double") && p[2] == "IDENTITY")
                        hatalitur = true;
                }
                if (p.Length == 3)
                {
                    if ((p[1] == "text" && p[2] != "CI") || (p[1] != "text" && p[2] == "CI"))
                        hatalitur2 = true;
                }
            }

            if (identity_count > 1)
            {
                File.Delete("Databases/" + db + "/table_creation.log");
                
                return "Tables can't contain more than one IDENTITY column!";
            }

            if (hatalitur)
            {
                File.Delete("Databases/" + db + "/table_creation.log");
                
                return "Tables's IDENTITY columns can't be text or double types!";
            }

            if (hatalitur2)
            {
                File.Delete("Databases/" + db + "/table_creation.log");
                
                return "Tables's CI can only apply to text type columns!";
            }

            for (int i = 0; i < cnt; ++i) {
                if (i != 0)
                    File.AppendAllText("Databases/" + db + "/" + tablename + ".ssf", "\r\n");

                String[] p = nlist[i].InnerText.Split(' ');

                if (p.Length == 3 && p[2] == "IDENTITY") {
                    if (ilk == 1) {
                        indexsection = "-----";
                        ilk = 0;
                    }
                    String filename1;
                    
                    do {    
                        filename1 = random_file_name();
                    } while (File.Exists("Databases/"+db+"/"+filename1+".indexdef"));
                    
                    indexsection += "\r\n" + filename1 +" UNIQUE";
                    myindex my_index = new myindex();
                    my_index.database_name = db;
                    my_index.file_path = "Databases/" + db + "/" + filename1 + ".indexdef";
                    my_index.index_name = filename1;
                    my_index.table_name = tablename;
                    my_index.unique = true;

                    String filename2;
                    do
                    {
                        filename2 = random_file_name();
                    } while (File.Exists("Databases/" + db + "/" + filename2 + ".ndx"));

                    mysubindex my_subindex = new mysubindex();
                    my_subindex.column_name = p[0];
                    my_subindex.file_path = "Databases/" + db + "/" + filename2 +".ndx";

                    File.Copy("index_template_int.ndx", my_subindex.file_path, true);
                    
                    my_subindex.id = filename2;
                    my_subindex.index_file = new SQLiteConnection("Data Source=" + my_subindex.file_path);
                    my_subindex.index_file.Open();

                    my_index.sub_indexes.Add(my_subindex);

                    if (i != 0)
                        File.AppendAllText(my_index.file_path, "\r\n");
                    File.AppendAllText(my_index.file_path, my_subindex.column_name + " " + my_subindex.id);

                    
                    indexes.Add(my_index);
                    

                    String incfile = "Databases/" + db + "/" + tablename + "." + p[0] + ".inc";
                    File.WriteAllText(incfile, "0");
                }

                File.AppendAllText("Databases/" + db + "/" + tablename + ".ssf", nlist[i].InnerText);
            }

            if (indexsection != "")
                File.AppendAllText("Databases/" + db + "/" + tablename + ".ssf", "\r\n" + indexsection);

            File.AppendAllText("Databases/" + db + "/" + tablename + ".ssd", "");

            mytable my_table = new mytable();
            my_table.columns_info = u_alan_info(db, tablename);
            my_table.data_file_path = "Databases/" + db + "/" + tablename + ".ssd";
            my_table.form_file_path = "Databases/" + db + "/" + tablename + ".ssf";
            my_table.insert_log_file_path = "Databases/" + db + "/" + tablename + ".logins";
            my_table.update_log_file_path = "Databases/" + db + "/" + tablename + ".logupd";
            my_table.delete_log_file_path = "Databases/" + db + "/" + tablename + ".logdel";

            my_table.database_name = db;
            my_table.table_name = tablename;

            
            tables.Add(my_table);
            

            File.AppendAllText("Databases/" + db + "/table_creation.log", "\r\nE");
            
            return "OK";
        }


        private void stop()
        {
            if (button1.Enabled == false)
            {
                Thread son_thread = new Thread(sonbaglanti);
                son_thread.Start(null);
                son_thread.Join();

                ana_thread.Join();

                for (var i = 0; i < indexes.Count; ++i)
                {
                    for (var k = 0; k < indexes[i].sub_indexes.Count(); ++k)
                    {
                        
                        indexes[i].sub_indexes[k].index_file.Close();
                        indexes[i].sub_indexes[k].index_file.Dispose();
                        
                    }
                }

                cache_slots_rw_lock.EnterWriteLock();

                for (int i = 0; i < cache_slots.Count; ++i)
                {
                    var mykey = cache_slots.ElementAt(i).Key;
                    cache_slots.Remove(mykey);
                    --i;
                }

                cache_slots_rw_lock.ExitWriteLock();

                db_slots_rw_lock.EnterWriteLock();

                for (int i = 0; i < db_slots.Count; ++i)
                {
                    var mykey = db_slots.ElementAt(i).Key;
                    db_slots.Remove(mykey);
                    --i;
                }

                db_slots_rw_lock.ExitWriteLock();
            }
        }

        private void button2_Click(object sender, EventArgs e)
        {
            stop();

            timer1.Enabled = false;
            timer_refresh_slots.Enabled = false;
            button2.Enabled = false;
            button1.Enabled = true;
            checkBox1.Enabled = true;
            progressBar1.Value = 0;
            label2.Text = "Status : Stopped";
            Application.DoEvents();
        }

        private void hakkındaToolStripMenuItem_Click(object sender, EventArgs e)
        {
            AboutBox1 f = new AboutBox1();
            f.ShowDialog();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            
        }

        private void Form1_Resize(object sender, EventArgs e)
        {
            if (FormWindowState.Minimized == this.WindowState)
            {
                notifyIcon1.Visible = true;
                notifyIcon1.ShowBalloonTip(500);
                this.Hide();
            }
            else if (FormWindowState.Normal == this.WindowState)
            {
                notifyIcon1.Visible = false;
            }
        }

        private void notifyIcon1_MouseDoubleClick(object sender, MouseEventArgs e)
        {
            this.Show();
            this.WindowState = FormWindowState.Normal;
        }

        public Process PriorProcess()
        // Returns a System.Diagnostics.Process pointing to
        // a pre-existing process with the same name as the
        // current one, if any; or null if the current process
        // is unique.
        {
            Process curr = Process.GetCurrentProcess();
            Process[] procs = Process.GetProcessesByName(curr.ProcessName);
            foreach (Process p in procs)
            {
                if ((p.Id != curr.Id) &&
                    (p.MainModule.FileName == curr.MainModule.FileName))
                    return p;
            }
            return null;
        }

        string MD5_encode(string str_encode)
        {
            MD5 md5Hash = MD5.Create();
            // Convert the input string to a byte array and compute the hash.
            byte[] data = md5Hash.ComputeHash(Encoding.UTF8.GetBytes(str_encode));

            // Create a new Stringbuilder to collect the bytes
            // and create a string.
            StringBuilder sBuilder = new StringBuilder();

            // Loop through each byte of the hashed data 
            // and format each one as a hexadecimal string.
            for (int i = 0; i < data.Length; i++)
            {
                sBuilder.Append(data[i].ToString("x2"));
            }

            // Return the hexadecimal string.
            return sBuilder.ToString();
        }

        public bool SQL_Like(string toSearch, string toFind)
        {
            return new Regex(@"\A" + new Regex(@"\.|\$|\{|\(|\||\)|\*|\+|\?|\\").Replace(toFind, ch => @"\" + ch).Replace('_', '.').Replace("%", ".*") + @"\z", RegexOptions.Singleline).IsMatch(toSearch);
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

        private String random_file_name() {
            String ret = "";
            String pattern = "0123456789abcdef";

            for (var i = 0; i < 8; ++i)
                ret += pattern.Substring(rnd.Next(0, 16), 1);

            return ret;
        }

        private String random_number()
        {
            return rnd.Next(1, Int32.MaxValue).ToString();
        }

        public static CultureInfo GetCurrentCulture() {
            return g_culture_info;
        }
        column_order_struct get_column_order(String column_name, ref List<select_column_struct> list_spreaded_select_columns)
        {
            column_order_struct ret = new column_order_struct();

            if (column_name == null) {
                ret.table_name = null;
                return ret;
            }

            var indis = 0;
            var curr_tablename = "";

            for (var k = 0; k < list_spreaded_select_columns.Count; ++k)
            {
                if (list_spreaded_select_columns[k].alias == column_name)
                {
                    ret.indis = 0;
                    ret.table_name = "\0";
                    ret.indis2 = k;
                    ret.ci = list_spreaded_select_columns[k].ci;
                    return ret;
                }

                if (list_spreaded_select_columns[k].name == "*")
                    continue;

                curr_tablename = list_spreaded_select_columns[k].table.table_name;
                indis = 0;

                for (var j = k + 1; j < list_spreaded_select_columns.Count; ++j)
                {
                    if (list_spreaded_select_columns[j].name != "*" && list_spreaded_select_columns[j].table.table_name == curr_tablename)
                        ++indis;
                    if (list_spreaded_select_columns[j].name != "*" && list_spreaded_select_columns[j].table.table_name == curr_tablename && list_spreaded_select_columns[j].name == column_name)
                    {
                        ret.indis = indis;
                        ret.table_name = list_spreaded_select_columns[j].table.table_name;
                        ret.indis2 = j;
                        ret.ci = list_spreaded_select_columns[j].ci;
                        return ret;
                    }
                }

                if (list_spreaded_select_columns[k].name == column_name)
                {
                    ret.indis = 0;
                    ret.table_name = curr_tablename;
                    ret.indis2 = k;
                    ret.ci = list_spreaded_select_columns[k].ci;
                    return ret;
                }
            }

            return ret;
        }

        select_column_struct get_column_struct_value(String column_name, ref List<select_column_struct> list_select_columns)
        {
            var ret = new select_column_struct();

            for (var i = 0; i < list_select_columns.Count; ++i)
                if (list_select_columns[i].name == column_name)
                {
                    ret = list_select_columns[i];
                    return ret;
                }

            return ret;
        }

        int get_table_order(String column_name, ref List<mytable> list_tables)
        {
            for (var i = 0; i < list_tables.Count; ++i) 
            {
                for (var k = 0; k < list_tables[i].columns_info.Count; ++k)
                {
                    if (list_tables[i].columns_info[k].name == column_name)
                        return i;
                }    
            }

            return -1;
        }

        private void Form1_FormClosing(object sender, FormClosingEventArgs e)
        {
            stop();
        }

        private void timer1_Tick(object sender, EventArgs e)
        {
            if (progressBar1.Value == 100)
                progressBar1.Value = 0;
            else
                progressBar1.Value += 1;
            Application.DoEvents();
        }

        private void timer_refresh_slots_Tick(object sender, EventArgs e)
        {
            cache_slots_rw_lock.EnterWriteLock();
            var suan = DateTime.Now;

            for (int i = 0; i < cache_slots.Count; ++i)
            {
                var mykey = cache_slots.ElementAt(i).Key;
                var elapsed = suan.Subtract(cache_slots[mykey].last_access_time).TotalSeconds;
                if (elapsed > Recordset_Activity_Timeout)
                {
                    cache_slots.Remove(mykey);
                    --i;
                }
            }
            
            cache_slots_rw_lock.ExitWriteLock();
        }

        public void db_lock(String dbname, int enter_exit, int read_write) {
            db_slots_rw_lock.EnterReadLock();

            var mylock = db_slots[dbname];

            if (mylock == null) {
                db_slots_rw_lock.ExitReadLock();
                return;
            }

            if (enter_exit == 0)
            {
                if (read_write == 0)
                    mylock.EnterReadLock();
                else
                    mylock.EnterWriteLock();
            }
            else {
                if (read_write == 0)
                    mylock.ExitReadLock();
                else
                    mylock.ExitWriteLock();
            }

            db_slots_rw_lock.ExitReadLock();
        }
    }

    [SQLiteFunction(FuncType = FunctionType.Collation, Name = "UTF8CI")]
    public class SQLiteCaseInsensitiveCollation : SQLiteFunction
    {
        /// <summary>
        /// CultureInfo for comparing strings in case insensitive manner 
        /// </summary>
        private static readonly CultureInfo _cultureInfo = Form1.GetCurrentCulture();

        /// <summary>
        /// Does case-insensitive comparison using _cultureInfo 
        /// </summary>
        /// <param name="x">Left string</param>
        /// <param name="y">Right string</param>
        /// <returns>The result of a comparison</returns>
        public override int Compare(string x, string y)
        {
            return string.Compare(x, y, _cultureInfo, CompareOptions.IgnoreCase);
        }
    }

    [SQLiteFunction(FuncType = FunctionType.Collation, Name = "UTF8CS")]
    public class SQLiteCaseSensitiveCollation : SQLiteFunction
    {
        /// <summary>
        /// CultureInfo for comparing strings in case insensitive manner 
        /// </summary>
        private static readonly CultureInfo _cultureInfo = Form1.GetCurrentCulture();

        /// <summary>
        /// Does case-insensitive comparison using _cultureInfo 
        /// </summary>
        /// <param name="x">Left string</param>
        /// <param name="y">Right string</param>
        /// <returns>The result of a comparison</returns>
        public override int Compare(string x, string y)
        {
            return string.Compare(x, y, _cultureInfo, CompareOptions.None);
        }
    }

    [SQLiteFunction(Name = "TOUPPER", Arguments = 1, FuncType = FunctionType.Scalar)]
    public class TOUPPER : SQLiteFunction
    {
        private static readonly CultureInfo _cultureInfo = Form1.GetCurrentCulture();

        public override object Invoke(object[] args)
        {
            return args[0].ToString().ToUpper(_cultureInfo);
        }
    }

    public struct AlanInfo {
        public String name;
        public String type;
        public Boolean ai;
        public Boolean ci;
    }

    public class mytable {
        public String table_name, data_file_path, database_name, form_file_path;
        public String insert_log_file_path, update_log_file_path, delete_log_file_path;
        //public ReaderWriterLockSlim file_lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        public List<AlanInfo> columns_info = new List<AlanInfo>();
    }
    public class myindex {
        public String index_name, table_name, file_path, database_name;
        public Boolean unique;
        public List<mysubindex> sub_indexes = new List<mysubindex>();
    }

    public class mysubindex {
        public String column_name, id, file_path;
        public SQLiteConnection index_file;
        //public ReaderWriterLockSlim file_lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
    }

    public struct myval
    {
        public sbyte mynull;
        public sbyte mychar;
        public Int16 myint16;
        public Int32 myint32;
        public Int64 myint64;
        public String mystring;
        public double mydouble;
        public sbyte type;
    }

    public struct select_column_struct
    {
        public String name;
        public String function;
        public String alias;
        public String type;
        public Boolean ai;
        public Boolean ci;
        public mytable table;
        public mysubindex index;
        public String value;
        public String op;
    }

    public struct column_order_struct
    {
        public String table_name;
        public int indis, indis2;
        public Boolean ci;
    }

    public struct rollback_struct
    {
        public SQLiteConnection conn;
        public String key;
        public String value;
        //public ReaderWriterLockSlim mylock;
        public String type;
    }
    public class CCacheSlot
    {
        public int curr_record_indis = 2;

        public List<List<Object>> records = new List<List<Object>>();

        public DateTime last_access_time;

        public CCacheSlot() {

        }
    }
}
