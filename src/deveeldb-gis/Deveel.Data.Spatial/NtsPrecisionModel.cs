using System;

using GeoAPI.Geometries;

using NetTopologySuite.Geometries;

namespace Deveel.Data.Spatial {
	public class NtsPrecisionModel : IPrecisionModel {
		internal GeoAPI.Geometries.IPrecisionModel WrappedPrecisionModel { get; private set; }

		public NtsPrecisionModel(PrecisionModelType modelType, int scale) {
			WrappedPrecisionModel = new PrecisionModel(scale);
		}

		public bool Equals(IPrecisionModel other) {
			var precisionModel = (NtsPrecisionModel) other;
			return WrappedPrecisionModel.Equals(precisionModel.WrappedPrecisionModel);
		}

		public int CompareTo(IPrecisionModel other) {
			var precisionModel = (NtsPrecisionModel)other;
			return precisionModel.WrappedPrecisionModel.CompareTo(precisionModel.WrappedPrecisionModel);
		}

		private static PrecisionModelType GetModelType(PrecisionModels models) {
			if (models == PrecisionModels.Fixed)
				return PrecisionModelType.Fixed;
			if (models == PrecisionModels.Floating)
				return PrecisionModelType.Floating;
			if (models == PrecisionModels.FloatingSingle)
				return PrecisionModelType.FloatingSingle;

			throw new InvalidOperationException();
		}

		public PrecisionModelType PrecisionModelType {
			get { return GetModelType(WrappedPrecisionModel.PrecisionModelType); }
		}

		public int MaximumSignificantDigits {
			get { return WrappedPrecisionModel.MaximumSignificantDigits; }
		}

		public double Scale {
			get { return WrappedPrecisionModel.Scale; }
		}

		public double MakePrecise(double value) {
			return WrappedPrecisionModel.MakePrecise(value);
		}

		public ICoordinate MakePrecise(ICoordinate coordinate) {
			throw new NotImplementedException();
		}
	}
}
