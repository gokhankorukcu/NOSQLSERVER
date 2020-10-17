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
    public partial class CreateDatabase : Form
    {
        private Form1 opener;

        public CreateDatabase(Form1 opener)
        {
            InitializeComponent();
            this.opener = opener;
        }

        private void button1_Click(object sender, EventArgs e)
        {
            if (opener.Conn == null)
            {
                MessageBox.Show("Open a database firstly!");
                return;
            }
            opener.Conn.createdatabase(textBox1.Text, textBox2.Text);

            if (opener.Conn.message == "OK")
                MessageBox.Show("Database has created.");
            else
                MessageBox.Show(opener.Conn.message);
            this.Close();
        }
    }
}
