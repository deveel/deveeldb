using System;

namespace Deveel.Data.Serialization {
	public interface ISerializable {
		void GetData(SerializeData data);
	}
}
