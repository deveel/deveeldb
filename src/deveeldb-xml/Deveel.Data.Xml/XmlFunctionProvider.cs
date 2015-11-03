using System;

using Deveel.Data.Deveel.Data.Xml;
using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Fluid;


namespace Deveel.Data.Xml {
	public sealed class XmlFunctionProvider : FunctionProvider {
		public override string SchemaName {
			get { return SystemSchema.Name; }
		}

		protected override ObjectName NormalizeName(ObjectName functionName) {
			if (functionName.Parent == null)
				functionName = new ObjectName(new ObjectName(SchemaName), functionName.Name);

			return base.NormalizeName(functionName);
		}

		private static ExecuteResult Simple(ExecuteContext context, Func<DataObject[], DataObject> func) {
			var args = context.EvaluatedArguments;
			var funcResult = func(args);
			return context.Result(funcResult);
		}

		protected override void OnInit() {
			Register(config => config.Named("to_xml")
				.WithStringParameter("s")
				.WhenExecute(context => Simple(context, args => XmlFunctions.XmlType(args[0])))
				.ReturnsType(XmlNodeType.XmlType));

			Register(config => config.Named("appendchildxml")
				.WithXmlParameter("obj")
				.WithStringParameter("xpath")
				.WithXmlParameter("value")
				.WhenExecute(context => Simple(context, args => XmlFunctions.AppendChild(args[0], args[1], args[2])))
				.ReturnsXmlType());

			Register(config => config.Named("extract")
				.WithXmlParameter("node")
				.WithStringParameter("xpath")
				.WhenExecute(context => Simple(context, args => XmlFunctions.Extract(args[0], args[1])))
				.ReturnsXmlType());

			Register(config => config.Named("extractvalue")
				.WithXmlParameter("node")
				.WithStringParameter("xpath")
				.WhenExecute(context => Simple(context, args => XmlFunctions.ExtractValue(args[0], args[1])))
				.ReturnsType(Function.DynamicType));

			Register(config => config.Named("existsxml")
				.WithXmlParameter("node")
				.WithStringParameter("xpath")
				.WhenExecute(context => Simple(context, args => XmlFunctions.Exists(args[0], args[1])))
				.ReturnsXmlType());

			Register(config => config.Named("insertbeforexml")
				.WithXmlParameter("node")
				.WithStringParameter("xpath")
				.WithXmlParameter("value")
				.WhenExecute(context => Simple(context, args => XmlFunctions.InsertBefore(args[0], args[1], args[2])))
				.ReturnsXmlType());

			Register(config => config.Named("insertchildxml")
				.WithXmlParameter("node")
				.WithStringParameter("xpath")
				.WithXmlParameter("child")
				.WithXmlParameter("value")
				.WhenExecute(context => Simple(context, args => XmlFunctions.InsertChild(args[0], args[1], args[2], args[3])))
				.ReturnsXmlType());

			Register(config => config.Named("updatexml")
				.WithXmlParameter("node")
				.WithStringParameter("xpath")
				.WithDynamicParameter("value")
				.WhenExecute(context => Simple(context, args => XmlFunctions.Update(args[0], args[1], args[2])))
				.ReturnsXmlType());
		}
	}
}
