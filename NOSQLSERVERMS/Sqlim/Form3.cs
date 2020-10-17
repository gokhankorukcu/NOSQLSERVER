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
    public partial class Form3 : Form
    {
        public Form1 opener;
        public String tablename, columnname;
        public int op; // 0 = başına, 1 = sonuna, 2 = sütundan sonra, 3 = degistir
        public NOSQLServerCommand Conn;
        public int degistirilicek_alanin_indisi;

        public Form3(Form1 opener, NOSQLServerCommand Conn, String tablename = "", String columnname = "", int op = 0, int degistirilicek_alanin_indisi = -1)
        {
            InitializeComponent();
            this.opener = opener;
            this.Conn = Conn;
            this.tablename = tablename;
            this.columnname = columnname;
            this.op = op;
            this.degistirilicek_alanin_indisi = degistirilicek_alanin_indisi;

            List<string> l = SQLiteGetList(opener.sconn, "SELECT name FROM DataTypes ORDER BY name");

            DataGridViewComboBoxColumn theColumn = (DataGridViewComboBoxColumn)dataGridView1.Columns["DataType"];

            for (int ii = 0; ii < l.Count(); ++ii)
            {
                theColumn.Items.Add(l[ii]);
            }


            if (tablename == "")
            {
                this.Text = "Create a Table";
                dataGridView1.AllowUserToAddRows = true;
            }



        }

        private void dataGridView1_RowEnter(object sender, DataGridViewCellEventArgs e)
        {

        }

        public List<string> SQLiteGetList(SQLiteConnection connection, string cmd)
        {
            List<string> QueryResult = new List<string>();
            SQLiteCommand cmdName = new SQLiteCommand(cmd, connection);
            SQLiteDataReader reader = cmdName.ExecuteReader();
            while (reader.Read())
            {
                QueryResult.Add(reader.GetString(0));
            }
            reader.Close();
            return QueryResult;
        }

        private void dataGridView1_RowsAdded(object sender, DataGridViewRowsAddedEventArgs e)
        {

        }

        private void dataGridView1_CellValueChanged(object sender, DataGridViewCellEventArgs e)
        {

        }

        private void dataGridView1_CellValidating(object sender, DataGridViewCellValidatingEventArgs e)
        {

        }

        private void dataGridView1_RowValidating(object sender, DataGridViewCellCancelEventArgs e)
        {

        }

        private void button1_Click(object sender, EventArgs e)
        {

            if (Conn == null)
            {
                MessageBox.Show("Open a database firstly!");
                return;
            }


            var list = new List<String>();
            var g = 0;

            for (int i = 0; i < dataGridView1.RowCount; ++i)
            {
                if (ks(dataGridView1.Rows[i].Cells["SutunIsmi"].Value) != "")
                {
                    String str = satir_hazirla(i);
                    list.Add(str);
                    g++;
                }
            }

            if (g == 0)
            {
                MessageBox.Show("There is no information to create table!");
                return;
            }

        
            Conn.createtable(textBox1.Text, list.ToArray()).execute();
            

            if ( Conn.message == "OK")
            {

                opener.gecerli_tablo = textBox1.Text;
                opener.tree_guncelle();
                opener.yapi_getir(textBox1.Text);


                MessageBox.Show("Table has created.");
            }
            else
                MessageBox.Show(Conn.message);

            this.Close();
        }

        private String satir_hazirla(int indis)
        {
            String ret = "";
            DataGridViewRow r = dataGridView1.Rows[indis];

            ret = String.Format("{0} {1}{2}",
                    r.Cells["SutunIsmi"].Value,
                    r.Cells["DataType"].Value,

                    ks(r.Cells["Ai"].Value) == "1" ? " IDENTITY" : ""

              );

            return ret;
        }

        public String ks(Object p)
        {
            if (p == null) return "";
            return p.ToString();
        }

        

        private TreeNode FindNode(TreeNode root, String name)
        {
            foreach (TreeNode node in root.Nodes)
            {
                if (node.Name == name)
                    return node;
                else
                {
                    if (node.Nodes.Count > 0)
                        return FindNode(node, name);
                }
            }
            return null;
        }


    }

}
