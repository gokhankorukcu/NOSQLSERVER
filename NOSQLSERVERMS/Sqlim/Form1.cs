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
    public partial class Form1 : Form
    {
        public String database, username="", password="", server="", port="";
        public TreeView tv;
        public NOSQLServerCommand Conn;
        public NOSQLServerCommand Conn2;
        

        public SQLiteConnection sconn = new SQLiteConnection("Data Source=Sqlim.sqlite;");

        public object[][] g_rows;
        public String gecerli_tablo="", gecerli_engine, gecerli_collation;
        public int gecerli_indis;

        public object[][] g_tablolar;
        public object[][] g_collations;

        public List<List<object>> g_indeksler = new List<List<object>>();
        public List<List<object>> g_foreigns = new List<List<object>>();

        private Boolean program = true;

        public Form1()
        {
            InitializeComponent();
            tv = treeView1;
            sconn.Open();
        }

        private void dosyaToolStripMenuItem_Click(object sender, EventArgs e)
        {

        }

        private void openDatabaseToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (Conn != null)
            {
                MessageBox.Show("Close active database firstly!");
                return;
            }

            Form2 f = new Form2(this);
            f.ShowDialog();

        }

        private void Form1_Load(object sender, EventArgs e)
        {

        }

        private void treeView1_NodeMouseDoubleClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            if (treeView1.SelectedNode == null || treeView1.SelectedNode.Parent == null)
                return;

            String t = treeView1.SelectedNode.Parent.Text;

            if (t != "Tables")
                return;

            if (treeView1.SelectedNode.Parent.Text == "Tables")
            {
                //tabControl1.SelectedIndex = 0;
                dataGridView3.CellValueChanged -= dataGridView3_CellValueChanged;

                yapi_getir(treeView1.SelectedNode.Text);

                dataGridView3.CellValueChanged += dataGridView3_CellValueChanged;

                gecerli_tablo = treeView1.SelectedNode.Text;
                label1.Text = "Table : " + gecerli_tablo;
            }
        }

        private void show_create(String parent, String text)
        {
            
        }

        public void yapi_getir(String tablename) {
            Conn.showtable(tablename).execute();



            int count = Conn.records.Count;
       

            g_rows = new object[count][];

            

            
            dataGridView1.Rows.Clear();
            

            
                for (var i = 0; i < Conn.records.Count; ++i)
                {
                    object[] p = new object[3];
                    g_rows[i] = new object[3];

                    var parcalar = Conn.records[i][0].Split(' ');

                    p[0] = parcalar[0];
                   
                    
                    p[1] = parcalar[1];
                    
                    p[2] = parcalar.Contains("IDENTITY") ? "1" : "0";
                   
                    dataGridView1.Rows.Insert(i, p);
                    p.CopyTo(g_rows[i], 0);
                }
                

                indexleri_getir(tablename);
                
        }

        private void Form1_FormClosing(object sender, FormClosingEventArgs e)
        {
            
            sconn.Close();
        }

        private void hakkındaToolStripMenuItem_Click(object sender, EventArgs e)
        {
            AboutBox1 f = new AboutBox1();
            f.ShowDialog();
        }

        private void tabloOluşturToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (Conn == null)
            {
                MessageBox.Show("Open a database firstly!");
                return;
            }
            Form3 f = new Form3(this, Conn);
            f.ShowDialog();
        }

        private String ks(Object p)
        {
            if (p == null) return "";
            return p.ToString();
        }

        private void dataGridView1_CellContentClick(object sender, DataGridViewCellEventArgs e)
        {
            
        }

        

        private void button1_Click(object sender, EventArgs e)
        {
           
        }

        private void button2_Click(object sender, EventArgs e)
        {
            
        }

        private void button3_Click(object sender, EventArgs e)
        {
            
        }

        private void button4_Click(object sender, EventArgs e)
        {
            if (gecerli_tablo == "")
                return;

            if (MessageBox.Show("Do you really want to drop `" + gecerli_tablo + "` table?\n\nWarning : All data will be lost for the this table!", "Warning", MessageBoxButtons.YesNo) == System.Windows.Forms.DialogResult.Yes)
            {
                tablo_sil(gecerli_tablo);
                gecerli_tablo = gecerli_collation = gecerli_engine = "";
               
                dataGridView1.Rows.Clear();
                dataGridView2.Rows.Clear();
                dataGridView3.DataSource = null;
                label1.Text = "";
            }
        }

        private void tablo_sil(String tablo)
        {
            Conn.deletetable(tablo).execute();

            tree_guncelle();
        }

       

        private void db_sil()
        {

            Conn.deletedatabase().execute();
            
            //sconn.Close();
            dataGridView1.Rows.Clear();
            dataGridView2.Rows.Clear();
            //dataGridView3.Rows.Clear();

            treeView1.Nodes.Clear();
            
            dataGridView3.DataSource = null;
            label1.Text = "";

            gecerli_tablo = gecerli_engine = gecerli_collation = "";
        }

        public void tree_guncelle()
        {
            TreeNode rn = treeView1.Nodes[0];
            TreeNode tables = rn.Nodes["Tables"];
            
            tables.Nodes.Clear();
            
            g_tablolar = null;

            List<List<String>> names = new List<List<String>>();

            Conn.showtables().execute();

            for (var i = 0; i < Conn.records.Count; ++i) {
                List<String> prc = new List<String>();
                prc.Add(Conn.records[i][0]);
                
                names.Add(prc);
            }
           

            if (names.Count > 0)
                g_tablolar = new object[names.Count][];

            TreeNode tn;

            for (int i = 0; i < names.Count; ++i)
            {
                g_tablolar[i] = new object[3];
                g_tablolar[i][0] = names[i][0];
                

                tn = tables.Nodes.Add(names[i][0], names[i][0]);
                tn.ImageIndex = 3;
                tn.SelectedImageIndex = 3;
            }
        }

        

        public void indexleri_getir(String tablename)
        {
            DataGridViewComboBoxColumn theColumn = (DataGridViewComboBoxColumn)dataGridView2.Columns["IndexType"];

            if (theColumn.Items.Count == 0)
            {
                theColumn.Items.Add("");
                theColumn.Items.Add("UNIQUE");
            }

            DataGridViewButtonColumn bc = (DataGridViewButtonColumn)dataGridView2.Columns["Detail"];
            bc.Text = "Detail";

            Conn.showindexes(tablename).execute();

            g_indeksler.Clear();
            dataGridView2.Rows.Clear();


            for (var i = 0; i < Conn.records.Count; ++i)
            {
                object[] p = new object[4];
                var parcalar = Conn.records[i][0].Split(' ');
                var cols = "";

                Conn2.showindex(parcalar[0]).execute();
                for (var k = 0; k < Conn2.records.Count; ++k)
                {
                    if (k != 0)
                        cols += ",";
                    var pa = Conn2.records[k][0].Split(' ');

                    cols +=  pa[1];
                }


                p[0] = parcalar[0];
                p[1] = cols;
                p[2] = "Detail";
                p[3] = parcalar.Contains("UNIQUE") ? "UNIQUE" : "";
                
     
                List<object> satir = p.ToList();
                g_indeksler.Add(satir);

                dataGridView2.Rows.Insert(i, p);
                
            }
            
        }

        public void foreign_getir(String tablename)
        {
           
        }

        private void button5_Click(object sender, EventArgs e)
        {
            if (Conn == null)
            {
                MessageBox.Show("Open a database firstly!");
                return;
            }

            for (int i = 0; i < dataGridView2.RowCount; ++i) {
                String iname = ks(dataGridView2.Rows[i].Cells["IndexName"].Value);
                String cols = ks(dataGridView2.Rows[i].Cells["Columns"].Value);
                String itype = ks(dataGridView2.Rows[i].Cells["IndexType"].Value);

                if (iname == "")
                    continue;

                
                if (i >= g_indeksler.Count)
                {
                    var p = cols.Split(',');

                    Conn.createindex(iname, gecerli_tablo, itype == "UNIQUE", p).execute();
                }
            }

            indexleri_getir(gecerli_tablo);
            
            MessageBox.Show("Change(s) has saved.");
        }

        private void dataGridView2_CellContentClick(object sender, DataGridViewCellEventArgs e)
        {
            if (gecerli_tablo == "")
            {
                MessageBox.Show("Please open a table firstly!");
                return;
            }

            if (e.ColumnIndex == 2)
            {
                IndexDetail f = new IndexDetail(this, e.RowIndex);
                f.ShowDialog();
            }
        }

        private bool delete_index(int row_index) {
            
            return false;
        }

        private bool delete_foreign(int row_index)
        {
            return false;
        }

        private void ındexOluşturToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (Conn == null)
            {
                MessageBox.Show("Open a database firstly!");
                return;
            }
            tabControl1.SelectedIndex = 1;
        }

        private void createForeignKeyToolStripMenuItem_Click(object sender, EventArgs e)
        {
            
        }

        private void dataGridView3_CellValueChanged(object sender, DataGridViewCellEventArgs e)
        {
            
        }

        private void dataGridView3_CellContentClick(object sender, DataGridViewCellEventArgs e)
        {
            
        }

        private void button6_Click(object sender, EventArgs e)
        {
            
        }

        

        private String Query_Click(String sql)
        {
            
                
            program = false;
            return "";
        }

        private Boolean Update_Click()
        {
            return false;
        }

        private void button7_Click(object sender, EventArgs e)
        {
            
        }

        private void listBox1_DoubleClick(object sender, EventArgs e)
        {
            
        }


        public void closedatabase()
        {
            dataGridView1.Rows.Clear();
            dataGridView2.Rows.Clear();
            //dataGridView3.Rows.Clear();

            treeView1.Nodes.Clear();
            
            
            dataGridView3.DataSource = null;

            gecerli_tablo = gecerli_engine = gecerli_collation = "";
        }

        private void closeDatabseToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (Conn == null)
            {
                MessageBox.Show("Open a database firstly!");
                return;
            }
            closedatabase();
            Conn = null;
            Conn2 = null;
        }

        private void createDatabaseToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (Conn == null)
            {
                MessageBox.Show("Open a database firstly!");
                return;
            }
            CreateDatabase f = new CreateDatabase(this);
            f.ShowDialog();
        }

        private void treeView1_KeyDown(object sender, KeyEventArgs e)
        {
            TreeNode sn = treeView1.SelectedNode;
            if (sn == null)
                return;

            if (sn.Parent == null) {
                if (MessageBox.Show("Do you really want to delete `" + database + "` database?\n\nWarning : All data of this database will be lost!", "Confirm", MessageBoxButtons.YesNo) == System.Windows.Forms.DialogResult.Yes)
                    db_sil();        
            } else if (sn.Parent.Text == "Tables") {
                if (MessageBox.Show("Do you really want to delete `"+ sn.Text +"` table?\n\nWarning : All data of this table will be lost!", "Confirm", MessageBoxButtons.YesNo) == System.Windows.Forms.DialogResult.Yes)
                    tablo_sil(sn.Text);
                    dataGridView1.Rows.Clear();
                    dataGridView2.Rows.Clear();
                    dataGridView3.DataSource = null;
                    label1.Text = "";
            }
            
        }

        private void createViewToolStripMenuItem_Click(object sender, EventArgs e)
        {
            
        }

        private void functionOluşturToolStripMenuItem_Click(object sender, EventArgs e)
        {
            
        }

        private void triggerOluşturToolStripMenuItem_Click(object sender, EventArgs e)
        {
            
        }

        private void eventOluşturToolStripMenuItem_Click(object sender, EventArgs e)
        {
            
        }

        private void button8_Click(object sender, EventArgs e)
        {
            
        }

        private void dataGridView4_CellValueChanged(object sender, DataGridViewCellEventArgs e)
        {
            
        }

        private void button8_Click_1(object sender, EventArgs e)
        {
            
            
        }

        private void dataGridView4_RowsAdded(object sender, DataGridViewRowsAddedEventArgs e)
        {
            
        }

        private void dataGridView4_RowsRemoved(object sender, DataGridViewRowsRemovedEventArgs e)
        {
            
        }

        private void ımportFromFileToolStripMenuItem_Click(object sender, EventArgs e)
        {
            
        }

        private void exportToFileToolStripMenuItem_Click(object sender, EventArgs e)
        {
            

            
        }

        private void treeView1_AfterSelect(object sender, TreeViewEventArgs e)
        {

        }

        private void button1_Click_1(object sender, EventArgs e)
        {
            if (Conn == null)
            {
                MessageBox.Show("Open a database firstly!");
                return;
            }

            Conn.select(gecerli_tablo, new String[] { "*" }).limit(int.Parse(textBox1.Text), int.Parse(textBox2.Text)).execute();

            if (Conn.message != "OK")
            {
                MessageBox.Show(Conn.message);
                return;
            }

            DataTable table = new DataTable();
            for (var i = 0; i < Conn.colnames.Count; ++i) {
                table.Columns.Add(Conn.colnames[i]);
            }

            for (var i = 0; i < Conn.records.Count; ++i)
            {
                DataRow row = table.NewRow();

                for (var k = 0; k < Conn.records[i].Count; ++k)
                    row.SetField(k, Conn.records[i][k]);

                table.Rows.Add(row);
            }

            dataGridView3.DataSource = table;
        }
    }
}
