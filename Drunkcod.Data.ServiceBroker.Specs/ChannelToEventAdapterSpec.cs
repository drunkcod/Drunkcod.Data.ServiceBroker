using System;
using System.Collections.Generic;
using Cone;

namespace Drunkcod.Data.ServiceBroker.Specs
{
	[Describe(typeof(ChannelToEventSourceAdapter<>))]
	public class ChannelToEventSourceAdapterSpec
	{
		IChannel<string> theChannel;
		ChannelToEventSourceAdapter<string> events;

		[BeforeEach]
		public void CreateChannelAndAdapter() {
			theChannel = new BlockingCollectionChannel<string>();
			events = new ChannelToEventSourceAdapter<string>(theChannel);
		}

		public void can_pump_events_when_noone_listens() {
			theChannel.Send("");
			Check.That(() => events.Pump(TimeSpan.Zero));
		}

		public void raises_event_when_message_received() {
			var messageReceived = false;
			var theMessage = "Helló World!";

			events.OnMessage += (s, e) => {
				messageReceived = true;
				Check.That(
					() => ReferenceEquals(s, events),
					() => e.Message == theMessage);
			};
			theChannel.Send(theMessage);
			Check.That(
				() => events.Pump(TimeSpan.Zero),
				() => messageReceived);
		}

		public void doesnt_break_handler_chain_on_exception() {
			var eventsRaised = 0;
			events.OnMessage += (s, e) => ++eventsRaised;
			events.OnMessage += (s, e) => { throw new InvalidOperationException(); };
			events.OnMessage += (s, e) => ++eventsRaised;

			theChannel.Send("");
			events.Pump(TimeSpan.Zero);
			Check.That(() => eventsRaised == 2);
		}

		public void raises_on_error_event_when_handler_throws() {
			var error = new InvalidOperationException();
			events.OnMessage += (s, e) => { throw error; };
			var onErrorRaised = false;
			events.OnError += (s, e) => {
				onErrorRaised = true;
				Check.That(() => ReferenceEquals(e.GetException(), error));
			};

			theChannel.Send("");
			events.Pump(TimeSpan.Zero);
			Check.That(() => onErrorRaised);
		}

		class CollectingObserver<T> : IObserver<T>
		{
			readonly List<T> seen = new List<T>(); 
			public int Count => seen.Count;

			public void OnNext(T value) { seen.Add(value); }

			public void OnError(Exception error)
			{
				throw new NotImplementedException();
			}

			public void OnCompleted()
			{
				throw new NotImplementedException();
			}
		}

		public void is_observable() {
			var observer = new CollectingObserver<string>();
			events.Subscribe(observer);

			theChannel.Send("Hello World!");
			events.Pump(TimeSpan.Zero);

			Check.That(() => observer.Count == 1);
		}

		public void can_unsubscribe() {
			var observer = new CollectingObserver<string>();
			var subscription = events.Subscribe(observer);

			theChannel.Send("Hello World!");
			events.Pump(TimeSpan.Zero);
			subscription.Dispose();
			theChannel.Send("Hello World!");
			events.Pump(TimeSpan.Zero);

			Check.That(() => observer.Count == 1);
		}
	}
}
