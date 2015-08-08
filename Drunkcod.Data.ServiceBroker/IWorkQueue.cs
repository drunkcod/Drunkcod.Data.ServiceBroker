using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Drunkcod.Data.ServiceBroker
{
	public interface IWorkQueue<T>
	{
		void Post(T item);
		bool Receive(Action<T> handleItem);
	}
}
