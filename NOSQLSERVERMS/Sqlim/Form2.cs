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
using NOSQLServerCommandNS;

namespace Sqlim
{
    public partial class Form2 : Form
    {
        public Form1 opener;
        public NOSQLServerCommand Conn;

        public Form2(Form1 opener)
        {
            InitializeComponent();
            this.opener = opener;

            SQLiteCommand cmd = new SQLiteCommand("SELECT * FROM ConnectionParams", opener.sconn);
            SQLiteDataReader r = cmd.ExecuteReader();

            if (r.Read())
            {
                textBox1.Text = r["server"].ToString();
                textBox3.Text = r["password"].ToString();
                textBox4.Text = r["port"].ToString();

                opener.server = r["server"].ToString();
                opener.password = r["password"].ToString();
                opener.port = r["port"].ToString();
            }
            r.Close();
        }

        private void charset_ve_collation_doldur()
        {


          
        }

        private void button1_Click(object sender, EventArgs e)
        {
            if (comboBox1.SelectedIndex == -1 && textBox2.Text == "") { 
                MessageBox.Show("Please choose a Database firstly!");
                return;
            }

            String sql = "UPDATE ConnectionParams SET server = '" + textBox1.Text + "', password='" + textBox3.Text.Replace("'","''") + "', port=" + textBox4.Text;
            SQLiteCommand cmd = new SQLiteCommand(sql, opener.sconn);
            
            try {
                cmd.ExecuteNonQuery();
            } catch (Exception ex) {
            
            }

            opener.server = textBox1.Text;
            opener.password = textBox3.Text;
            opener.port = textBox4.Text;
            opener.database = comboBox1.Text;
            if (comboBox1.SelectedIndex == -1)
                opener.database = textBox2.Text;



            Conn = new NOSQLServerCommand(opener.database, opener.password, opener.server, int.Parse(opener.port));


            opener.Conn = new NOSQLServerCommand(opener.database, opener.password, opener.server, int.Parse(opener.port));


            opener.Conn2 = new NOSQLServerCommand(opener.database, opener.password, opener.server, int.Parse(opener.port));


            opener.Conn.isdbexist().execute();

            if (opener.Conn.message != "OK") {
                MessageBox.Show(opener.Conn.message);
                return;
            }

            opener.closedatabase();

            tree_doldur();

            this.Close();
        }

        private void button2_Click(object sender, EventArgs e)
        {
            Conn = new NOSQLServerCommand("", textBox3.Text, textBox1.Text, int.Parse(textBox4.Text));


            var dbNames = new List<string>();

        
            textBox1.Enabled = false;
            textBox3.Enabled = false;
            textBox4.Enabled = false;

            Conn.showdatabases().execute();

            if (Conn.message != "OK")
            {
                MessageBox.Show(Conn.message);
                textBox1.Enabled = true;
                textBox3.Enabled = true;
                textBox4.Enabled = true;
                return;
            }
            else {
                for (var i = 0; i < Conn.records.Count; ++i)
                    dbNames.Add(Conn.records[i][0]);
            }
          
            comboBox1.Items.Clear();
            for (int i = 0; i < dbNames.Count; ++i)
                comboBox1.Items.Add(dbNames[i]);

            if (dbNames.Count > 0)
                MessageBox.Show("Database has listed. Please choose one");
            else
                MessageBox.Show("There are no Databases!");
        }

        

        public void tree_doldur()
        {
            opener.tv.Nodes.Clear();
            opener.g_tablolar = null;

            TreeNode root = opener.tv.Nodes.Add(opener.server + "(" + opener.database + ")");
            root.ImageIndex = 1;
            root.SelectedImageIndex = 1;
            TreeNode tables = root.Nodes.Add("Tables", "Tables");
            tables.ImageIndex = 2;
            tables.SelectedImageIndex = 2;
            

            List<List<String>> names = new List<List<String>>();

            Conn.showtables().execute();

            for (var i = 0; i < Conn.records.Count; ++i) {
                List<String> sat = new List<String>();
                sat.Add(Conn.records[i][0].ToString());
                names.Add(sat);
            }
            

            TreeNode tn;

            if (names.Count > 0)
                opener.g_tablolar = new object[names.Count][];
            
            for (int i = 0; i < names.Count; ++i)
            {
                opener.g_tablolar[i] = new object[3];
                opener.g_tablolar[i][0] = names[i][0];
                

                tn = tables.Nodes.Add(names[i][0]);
                tn.ImageIndex = 3;
                tn.SelectedImageIndex = 3;
            }
        }
    }
}
