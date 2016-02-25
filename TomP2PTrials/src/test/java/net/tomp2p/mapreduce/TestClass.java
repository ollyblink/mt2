package net.tomp2p.mapreduce;

import java.io.Serializable;

public class TestClass implements Serializable {
	Runnable r = new Runnable() {

		@Override
		public void run() {
			// TODO Auto-generated method stub

		}
	};

	class InnerTestClass implements Serializable {
		Runnable r = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub

			}
		};
	}

	static class InnerStaticTestClass implements Serializable {

	}

	interface InnerTestInterface extends Serializable {

	}

	public class AnonoymousContainers {
		Runnable r = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				Runnable r = new Runnable() {

					@Override
					public void run() {
						// TODO Auto-generated method stub
						Runnable r = new Runnable() {

							@Override
							public void run() {
								// TODO Auto-generated method stub
								Runnable r = new Runnable() {

									@Override
									public void run() {
										// TODO Auto-generated method stub

									}
								};
								Runnable r2 = new Runnable() {

									@Override
									public void run() {
										// TODO Auto-generated method stub

									}
								};
								Runnable r3 = new Runnable() {

									@Override
									public void run() {
										// TODO Auto-generated method stub

									}
								};
							}
						};
					}
				};
			}
		};
		Runnable r2 = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				Runnable r = new Runnable() {

					@Override
					public void run() {
						// TODO Auto-generated method stub

					}
				};
			}
		};
		Runnable r3 = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub

			}
		};
	}

	public void print() {
		System.out.println("Hello World");
	}
}
