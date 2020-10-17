using System;
using System.Text;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.Reflection;
using Microsoft.CSharp;
using System.Collections.Generic;

namespace EvalCSCodeNS
{
	public class EvalCSCode
	{
        public IEnumerable<List<Object>> EvalWithParams(string sCSCode, string pstr, List<List<List<Object>>> p1, string p2)
        {
            CSharpCodeProvider c = new CSharpCodeProvider();

            CompilerParameters cp = new CompilerParameters();

            cp.ReferencedAssemblies.Add("system.dll");
            cp.ReferencedAssemblies.Add("system.xml.dll");
            cp.ReferencedAssemblies.Add("System.Core.dll");
            cp.ReferencedAssemblies.Add("System.Data.Linq.dll");

            cp.CompilerOptions = "/t:library";
            cp.GenerateInMemory = true;
            StringBuilder sb = new StringBuilder("");

            sb.Append("using System;\n");
            sb.Append("using System.Xml;\n");
            sb.Append("using System.Linq;\n");
            sb.Append("using System.Collections.Generic;\n");

            sb.Append("namespace CSCodeEvaler{ \n");
            sb.Append("public class CSCodeEvaler{ \n");
            sb.Append("public IEnumerable<List<Object>> EvalCode(" + pstr + "){\n");
            sb.Append(sCSCode + " \n");
            sb.Append("return null; \n");
            sb.Append("} \n");
            sb.Append("} \n");
            sb.Append("}\n");
            //Debug.WriteLine(sb.ToString())// ' look at this to debug your eval string

            CompilerResults cr = c.CompileAssemblyFromSource(cp, sb.ToString());
            if (cr.Errors.Count > 0)
            {
                //MessageBox.Show("ERROR: "+ "Error evaluating cs code: " + cr.Errors[0].ErrorText);
                return null;
            }

            System.Reflection.Assembly a = cr.CompiledAssembly;
            object o = a.CreateInstance("CSCodeEvaler.CSCodeEvaler");
            Type t = o.GetType();
            MethodInfo mi = t.GetMethod("EvalCode");
            object[] oParams = new object[2];

            oParams[0] = p1;
            oParams[1] = p2;

            return (IEnumerable<List<Object>>) mi.Invoke(o, oParams);
        }
	}
}