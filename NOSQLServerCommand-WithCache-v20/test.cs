using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;
using NOSQLServerCommandNS;

namespace sstest1
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            NOSQLServerCommand c = new NOSQLServerCommand("", "gokhan", "127.0.0.1");
            c.createdatabase("testdb", "testpassword").execute();
            
            c = new NOSQLServerCommand("testdb", "testpassword", "127.0.0.1");
	    
			var param = new String[3];

            param[0] = "per_id int IDENTITY";
            param[1] = "per_name text CI";
            param[2] = "per_vehicleid int";
            
            c.createtable("persons", param).execute();

            c.insertinto("persons", new string[] { "per_name", "per_vehicleid" }).values(new string[] { "John", "1" }).execute();
            c.insertinto("persons", new string[] { "per_name", "per_vehicleid" }).values(new string[] { "Peter", "2" }).execute();
			c.insertinto("persons", new string[] { "per_name", "per_vehicleid" }).values(new string[] { "Peter", "3" }).execute();


            c.update("persons", new string[] { "per_name" }).values(new string[] { "Gokhan" }).where("per_name == 'John'", false).execute();
            
            var r = c.select("persons", new string[] {"*"}).orderby(new string[] {"per_name"}, new string[] {"desc"}).execute_reader();

            MessageBox.Show(c.message);

			var ret = "";
			if (r != null)
				while (r.fetch_next_record()) {
					for (int i = 0; i < r.record.Count; ++i)
						ret += r.record[i] + " | ";
					ret += "\n";
				}
			MessageBox.Show(ret);

			var param2 = new String[2];
			param2[0] = "veh_id int IDENTITY";
            param2[1] = "veh_name text";
            
			c.createtable("vehicles", param2).execute();

			c.createindex("ndx_vehicles_01", "vehicles", true, new string[] { "veh_name" }).execute();

			c.insertinto("vehicles", new string[] { "veh_name" }).values(new string[] { "BMW" }).execute();
			c.insertinto("vehicles", new string[] { "veh_name" }).values(new string[] { "Mercedes" }).execute();


			r = c.select("persons", new string[] { "*" }).join("vehicles", "per_vehicleid == veh_id", "left").where("per_name == 'Peter' && per_vehicleid < 3", true).where("per_vehicleid > 1", false).execute_reader();

			ret = "";
            if (r != null)
				while (r.fetch_next_record()) {
					for (int i = 0; i < r.record.Count; ++i)
						ret += r.record[i] + " | ";
					ret += "\n";	
				}
			MessageBox.Show(ret);
		
			r = c.select("persons", new string[] { "per_name", "count(*) as cnt" }).groupby("per_name").having("cnt > 1").orderby(new string[] {"cnt"}).limit(0, 10).execute_reader();

			ret = "";
            if (r != null)
				while (r.fetch_next_record()) {
					for (int i = 0; i < r.record.Count; ++i)
						ret += r.record[i] + " | ";
					ret += "\n";
				}
			MessageBox.Show(ret);
        }
    }
}