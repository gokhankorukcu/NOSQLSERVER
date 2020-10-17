using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Sqlim
{
    public partial class IndexDetail : Form
    {
        public Form1 opener;
        public int GridRowIndis;

        private String ks(Object p)
        {
            if (p == null) return "";
            return p.ToString();
        }

        public IndexDetail(Form1 opener, int GridRowIndis)
        {
            InitializeComponent();
            this.opener = opener;
            this.GridRowIndis = GridRowIndis;

            this.Text = "Detail of `"+ ks(opener.dataGridView2.Rows[GridRowIndis].Cells["IndexName"].Value) +"` index on table `"+ opener.gecerli_tablo +"`";

            String[] p = ks(opener.dataGridView2.Rows[GridRowIndis].Cells["Columns"].Value).Split(new[] { ", " }, StringSplitOptions.None);

            for (int i = 0; i < opener.g_rows.Count(); ++i) {
                object[] o = new object[2];

                o[1] = opener.g_rows[i][0];

                String bulundu = "0";
                for (int k = 0; k < p.Count(); ++k )
                    if (p[k] == (string) o[1]) {
                        bulundu = "1";
                        break;
                    }

                o[0] = bulundu;

                dataGridView1.Rows.Insert(i, o);
            }
        }

        private void button1_Click(object sender, EventArgs e)
        {
            String val = "";

            for (int i = 0; i < dataGridView1.Rows.Count; ++i)
                if ((string) dataGridView1.Rows[i].Cells["Select"].Value == "1")
                    val += "" + dataGridView1.Rows[i].Cells["ColumnName"].Value + ",";

            if (val != "")
                val = val.Substring(0, val.Length - 1);
            else
            {
                MessageBox.Show("You must select least one column!");
                return;
            }
            opener.dataGridView2.Rows[GridRowIndis].Cells["Columns"].Value = val;

            this.Close();
        }

        private void dataGridView1_CellContentClick(object sender, DataGridViewCellEventArgs e)
        {

        }
    }
}
