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

		protected override void OnInit() {
			New("from_wkb")
				.WithParameter(p => p.Named("source").OfType(PrimitiveTypes.Binary()))
				.WhenExecute(context => {
					var arg = context.EvaluatedArguments[0];
					return context.Result(SpatialSystemFunctions.FromWkb(arg));
				})
				.ReturnsType(SpatialType.Geometry());

			New("from_wkt")
				.WithParameter(p => p.Named("source").OfType(PrimitiveTypes.String()))
				.WhenExecute(context => {
					var arg = context.EvaluatedArguments[0];
					return context.Result(SpatialSystemFunctions.FromWkt(arg));
				})
				.ReturnsType(SpatialType.Geometry());

			// TODO: Implement the functions
		}
	}
}
