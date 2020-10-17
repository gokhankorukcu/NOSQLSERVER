using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Data.SQLite;
using System.IO;
using System.Globalization;
using System.Threading;

namespace SQLSunucu
{
    public partial class create_indexes : Form
    {
        mytable curr_table;
        Form1 opener;
        public create_indexes(mytable curr_table, Form1 opener)
        {
            this.curr_table = curr_table;
            this.opener = opener;
            InitializeComponent();

            CultureInfo customCulture = CultureInfo.CreateSpecificCulture(opener.cultureinfostr);
            customCulture.NumberFormat.NumberDecimalSeparator = ".";
            Thread.CurrentThread.CurrentCulture = customCulture;
        }

        private void start()
        {
            var indexes = opener.getindexes(curr_table.database_name, curr_table.table_name);
            var indisler = new List<int>();

            progressBar1.Maximum = opener.curr_whole_records[0].Count;
           
            for (var i = 0; i < indexes.Count; ++i) 
            {
                label1.Text = (i + 1) + ". index is rebuiding / Total " + indexes.Count + " indexes";
                indisler = new List<int>();
                for (var k = 0; k < indexes[i].sub_indexes.Count; ++k) 
                {
                    for (var j = 0; j < curr_table.columns_info.Count; ++j)
                    {
                        if (curr_table.columns_info[j].name == indexes[i].sub_indexes[k].column_name)
                        {
                            indisler.Add(j);
                            indexes[i].sub_indexes[k].index_file.Dispose();
                            indexes[i].sub_indexes[k].index_file = null;

                            File.Delete(indexes[i].sub_indexes[k].file_path);
                            if (curr_table.columns_info[j].type == "text" && curr_table.columns_info[j].ci == true)
                                File.Copy("index_template_text_ci.ndx", indexes[i].sub_indexes[k].file_path);
                            else if (curr_table.columns_info[j].type == "text" && curr_table.columns_info[j].ci == false)
                                File.Copy("index_template_text_cs.ndx", indexes[i].sub_indexes[k].file_path);
                            else if (curr_table.columns_info[j].type == "double")
                                File.Copy("index_template_double.ndx", indexes[i].sub_indexes[k].file_path);
                            else
                                File.Copy("index_template_int.ndx", indexes[i].sub_indexes[k].file_path);
                            indexes[i].sub_indexes[k].index_file = new SQLiteConnection("Data Source=" + indexes[i].sub_indexes[k].file_path);
                            indexes[i].sub_indexes[k].index_file.Open();
                        }
                    }
                }

                for (var w = 0; w < opener.curr_whole_records[0].Count; ++w)
                {
                    for (var k = 0; k < indexes[i].sub_indexes.Count; ++k)
                    {
                        var curr_index = "";
                        if (opener.curr_whole_records[0][w][indisler[k]] == null)
                            curr_index = "NULL";
                        else
                            curr_index = "'" + opener.curr_whole_records[0][w][indisler[k]].ToString().Replace("'", "''") + "'";
                        var curr_value = opener.curr_whole_records[0][w][opener.curr_whole_records[0][0].Count-1].ToString();
                        
                        var sql = "INSERT INTO keyvalue VALUES ("+ curr_index +", "+ curr_value +")";
                        var cmd = new SQLiteCommand(sql, indexes[i].sub_indexes[k].index_file);
                        if (cmd.ExecuteNonQuery() != 1)
                        {
                            MessageBox.Show("Index can't be builded!", "Error", MessageBoxButtons.OK,  MessageBoxIcon.Error);
                            opener.curr_ret = false;
                            this.Close();
                        }
                        cmd.Dispose();
                    }
                    progressBar1.Value = w + 1;
                    Application.DoEvents();
                }
            }

            for (var i = 0; i < indexes.Count; ++i)
            { 
                for (var k = 0; k < indexes[i].sub_indexes.Count; ++k)
                {
                    indexes[i].sub_indexes[k].index_file.Close();
                }
            }

            opener.curr_ret = true;
            this.Close();
        }

        private void create_indexes_Load(object sender, EventArgs e)
        {
            this.Show();
            start();
        }
    }
}
