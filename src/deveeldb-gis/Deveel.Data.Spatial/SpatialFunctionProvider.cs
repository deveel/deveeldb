using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Routines;
using Deveel.Data.Sql.Fluid;
using Deveel.Data.Types;

namespace Deveel.Data.Spatial {
	public sealed class SpatialFunctionProvider : FunctionProvider {
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
			Register(config => config.Named("from_wkb")
				.WithParameter(p => p.Named("source").OfType(PrimitiveTypes.Binary()))
				.WhenExecute(context => Simple(context, args => SpatialSystemFunctions.FromWkb(args[0])))
				.ReturnsType(SpatialType.Geometry()));

			Register(config => config.Named("from_wkt")
				.WithParameter(p => p.Named("source").OfType(PrimitiveTypes.String()))
				.WhenExecute(context => Simple(context, args => SpatialSystemFunctions.FromWkt(context.QueryContext, args[0])))
				.ReturnsType(SpatialType.Geometry()));

			Register(config => config.Named("to_wkt")
				.WithParameter(p => p.Named("g").OfType(SpatialType.Geometry()))
				.WhenExecute(context => Simple(context, args => SpatialSystemFunctions.ToWkt(args[0])))
				.ReturnsString());

			Register(config => config.Named("to_wkb")
				.WithParameter(p => p.Named("g").OfType(SpatialType.Geometry()))
				.WhenExecute(context => Simple(context, args => SpatialSystemFunctions.ToWkb(args[0])))
				.ReturnsBinary());

			Register(config => config.Named("envelope")
				.WithParameter(p => p.Named("g").OfType(SpatialType.Geometry()))
				.WhenExecute(context => Simple(context, args => SpatialSystemFunctions.Envelope(args[0])))
				.ReturnsType(SpatialType.Geometry()));

			Register(config => config.Named("distance")
				.WithParameter(p => p.Named("g").OfType(SpatialType.Geometry()))
				.WhenExecute(context => Simple(context, args => SpatialSystemFunctions.Distance(args[0])))
				.ReturnsNumeric());

			// TODO: Implement the functions
		}
	}
}
