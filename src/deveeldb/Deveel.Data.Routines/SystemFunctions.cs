using System;

namespace Deveel.Data.Routines {
	public static class SystemFunctions {
		private static FunctionFactory factory;

		public static TObject Abs(TObject ob) {
			if (ob.IsNull)
				return ob;

			return TObject.CreateBigNumber(ob.ToBigNumber().Abs());
		}

		public static FunctionFactory Factory {
			get {
				if (factory == null) {
					factory = new SystemFunctionsFactory();
					factory.Init();
				}

				return factory;
			}
		}

		#region SystemFunctionsFactory

		class SystemFunctionsFactory : FunctionFactory {
			public override void Init() {
			}
		}

		#endregion
	}
}