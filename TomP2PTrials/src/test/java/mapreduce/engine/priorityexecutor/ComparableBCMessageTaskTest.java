package mapreduce.engine.priorityexecutor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import mapreduce.engine.broadcasting.messages.BCMessageStatus;
import mapreduce.engine.multithreading.ComparableBCMessageTask;
import mapreduce.execution.jobs.PriorityLevel;

public class ComparableBCMessageTaskTest {

	@Test
	public void test() {
		List<ComparableBCMessageTask<Integer>> tasks = new ArrayList<>();
		Runnable r = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub

			}

		};
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(0), 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.LOW, new Long(1), 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));

		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), 0, BCMessageStatus.COMPLETED_PROCEDURE,
				new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), 0, BCMessageStatus.COMPLETED_PROCEDURE,
				new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), 0, BCMessageStatus.COMPLETED_PROCEDURE,
				new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), 0, BCMessageStatus.COMPLETED_PROCEDURE,
				new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), 1, BCMessageStatus.COMPLETED_PROCEDURE,
				new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(0), 1, BCMessageStatus.COMPLETED_PROCEDURE,
				new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), 1, BCMessageStatus.COMPLETED_PROCEDURE,
				new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.MODERATE, new Long(1), 1, BCMessageStatus.COMPLETED_PROCEDURE,
				new Long(1)));

		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), 0, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), 0, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), 1, BCMessageStatus.COMPLETED_TASK, new Long(0)));
		tasks.add(new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), 1, BCMessageStatus.COMPLETED_TASK, new Long(1)));
		tasks.add(
				new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(
				new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(
				new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(
				new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), 0, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(
				new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(
				new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(0), 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));
		tasks.add(
				new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(0)));
		tasks.add(
				new ComparableBCMessageTask<Integer>(r, null, PriorityLevel.HIGH, new Long(1), 1, BCMessageStatus.COMPLETED_PROCEDURE, new Long(1)));

		Collections.sort(tasks);
//		for (ComparableBCMessageTask<Integer> o : tasks) {
//			System.out.println(o.toString());
//		}

		for (int i = 0; i < tasks.size(); ++i) {
			if (i >= 0 && i < 16) {
				assertEquals(PriorityLevel.HIGH, tasks.get(i).getJobPriority());
				if (i >= 0 && i < 8) {
					assertEquals(new Long(0), tasks.get(i).getJobCreationTime());
					if (i >= 0 && i < 4) {
						assertEquals(new Integer(1), tasks.get(i).getProcedureIndex());
						if (i >= 0 && i < 2) {
							assertEquals(BCMessageStatus.COMPLETED_PROCEDURE, tasks.get(i).getMessageStatus());
							if (i == 0) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 1) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						} else if (i >= 2 && i < 4) {
							assertEquals(BCMessageStatus.COMPLETED_TASK, tasks.get(i).getMessageStatus());
							if (i == 2) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 3) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						}
					} else if (i >= 4 && i < 8) {
						assertEquals(new Integer(0), tasks.get(i).getProcedureIndex());
						if (i >= 4 && i < 6) {
							assertEquals(BCMessageStatus.COMPLETED_PROCEDURE, tasks.get(i).getMessageStatus());
							if (i == 4) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 5) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						} else if (i >= 6 && i < 8) {
							assertEquals(BCMessageStatus.COMPLETED_TASK, tasks.get(i).getMessageStatus());
							if (i == 6) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 7) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						}
					}
				} else if (i >= 8 && i < 16) {
					assertEquals(new Long(1), tasks.get(i).getJobCreationTime());
					if (i >= 8 && i < 12) {
						assertEquals(new Integer(1), tasks.get(i).getProcedureIndex());
						if (i >= 8 && i < 10) {
							assertEquals(BCMessageStatus.COMPLETED_PROCEDURE, tasks.get(i).getMessageStatus());
							if (i == 8) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 9) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						} else if (i >= 10 && i < 12) {
							assertEquals(BCMessageStatus.COMPLETED_TASK, tasks.get(i).getMessageStatus());
							if (i == 10) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 12) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						}
					} else if (i >= 12 && i < 16) {
						assertEquals(new Integer(0), tasks.get(i).getProcedureIndex());
						if (i >= 12 && i < 14) {
							assertEquals(BCMessageStatus.COMPLETED_PROCEDURE, tasks.get(i).getMessageStatus());
							if (i == 12) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 13) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						} else if (i >= 14 && i < 16) {
							assertEquals(BCMessageStatus.COMPLETED_TASK, tasks.get(i).getMessageStatus());
							if (i == 14) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 16) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						}
					}
				}
			} else if (i >= 16 && i < 32) {
				assertEquals(PriorityLevel.MODERATE, tasks.get(i).getJobPriority());
				if (i >= 16 && i < 24) {
					assertEquals(new Long(0), tasks.get(i).getJobCreationTime());
					if (i >= 16 && i < 20) {
						assertEquals(new Integer(1), tasks.get(i).getProcedureIndex());
						if (i >= 16 && i < 18) {
							assertEquals(BCMessageStatus.COMPLETED_PROCEDURE, tasks.get(i).getMessageStatus());
							if (i == 16) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 17) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						} else if (i >= 18 && i < 20) {
							assertEquals(BCMessageStatus.COMPLETED_TASK, tasks.get(i).getMessageStatus());
							if (i == 18) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 19) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						}
					} else if (i >= 20 && i < 24) {
						assertEquals(new Integer(0), tasks.get(i).getProcedureIndex());
						if (i >= 20 && i < 22) {
							assertEquals(BCMessageStatus.COMPLETED_PROCEDURE, tasks.get(i).getMessageStatus());
							if (i == 20) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 21) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						} else if (i >= 22 && i < 24) {
							assertEquals(BCMessageStatus.COMPLETED_TASK, tasks.get(i).getMessageStatus());
							if (i == 22) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 23) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						}
					}
				} else if (i >= 24 && i < 32) {
					assertEquals(new Long(1), tasks.get(i).getJobCreationTime());
					if (i >= 24 && i < 28) {
						assertEquals(new Integer(1), tasks.get(i).getProcedureIndex());
						if (i >= 24 && i < 26) {
							assertEquals(BCMessageStatus.COMPLETED_PROCEDURE, tasks.get(i).getMessageStatus());
							if (i == 24) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 25) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						} else if (i >= 26 && i < 28) {
							assertEquals(BCMessageStatus.COMPLETED_TASK, tasks.get(i).getMessageStatus());
							if (i == 26) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 27) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						}
					} else if (i >= 28 && i < 32) {
						assertEquals(new Integer(0), tasks.get(i).getProcedureIndex());
						if (i >= 28 && i < 30) {
							assertEquals(BCMessageStatus.COMPLETED_PROCEDURE, tasks.get(i).getMessageStatus());
							if (i == 28) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 29) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						} else if (i >= 30 && i < 32) {
							assertEquals(BCMessageStatus.COMPLETED_TASK, tasks.get(i).getMessageStatus());
							if (i == 30) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 31) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						}
					}
				}

			} else if (i >= 32 && i < 48) {
				assertEquals(PriorityLevel.LOW, tasks.get(i).getJobPriority());
				if (i >= 32 && i < 40) {
					assertEquals(new Long(0), tasks.get(i).getJobCreationTime());
					if (i >= 32 && i < 36) {
						assertEquals(new Integer(1), tasks.get(i).getProcedureIndex());
						if (i >= 32 && i < 34) {
							assertEquals(BCMessageStatus.COMPLETED_PROCEDURE, tasks.get(i).getMessageStatus());
							if (i == 32) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 33) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						} else if (i >= 36 && i < 38) {
							assertEquals(BCMessageStatus.COMPLETED_TASK, tasks.get(i).getMessageStatus());
							if (i == 36) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 37) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						}
					} else if (i >= 36 && i < 40) {
						assertEquals(new Integer(0), tasks.get(i).getProcedureIndex());
						if (i >= 36 && i < 38) {
							assertEquals(BCMessageStatus.COMPLETED_PROCEDURE, tasks.get(i).getMessageStatus());
							if (i == 36) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 37) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						} else if (i >= 38 && i < 40) {
							assertEquals(BCMessageStatus.COMPLETED_TASK, tasks.get(i).getMessageStatus());
							if (i == 38) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 39) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						}
					}
				} else if (i >= 40 && i < 48) {
					assertEquals(new Long(1), tasks.get(i).getJobCreationTime());
					if (i >= 40 && i < 44) {
						assertEquals(new Integer(1), tasks.get(i).getProcedureIndex());
						if (i >= 40 && i < 42) {
							assertEquals(BCMessageStatus.COMPLETED_PROCEDURE, tasks.get(i).getMessageStatus());
							if (i == 40) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 41) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						} else if (i >= 42 && i < 44) {
							assertEquals(BCMessageStatus.COMPLETED_TASK, tasks.get(i).getMessageStatus());
							if (i == 42) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 43) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						}
					} else if (i >= 44 && i < 48) {
						assertEquals(new Integer(0), tasks.get(i).getProcedureIndex());
						if (i >= 44 && i < 46) {
							assertEquals(BCMessageStatus.COMPLETED_PROCEDURE, tasks.get(i).getMessageStatus());
							if (i == 44) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 45) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						} else if (i >= 46 && i < 48) {
							assertEquals(BCMessageStatus.COMPLETED_TASK, tasks.get(i).getMessageStatus());
							if (i == 46) {
								assertEquals(new Long(0), tasks.get(i).getMessageCreationTime());
							} else if (i == 47) {
								assertEquals(new Long(1), tasks.get(i).getMessageCreationTime());
							}
						}
					}
				}

			}
		}
	}
}

//
// PriorityExecutor executor = PriorityExecutor.newFixedThreadPool(2);
//
// int count = 0;
// boolean[] trueValues = new boolean[6];
//
// for (int i = 0; i < 6; ++i) {
// trueValues[i] = false;
// }
//
//
// for (int i = 0; i < 6; ++i) {
// assertEquals(false, trueValues[i]);
// }
// for (Task task : tasks) {
// int counter = count++;
// executor.submit(new Runnable() {
//
// @Override
// public void run() {
// System.err.println(idOrder[counter] + "," + Integer.parseInt(task.key()));
// trueValues[counter] = idOrder[counter] == Integer.parseInt(task.key());
// try {
// Thread.sleep(10);
// } catch (InterruptedException e) {
// // TODO Auto-generated catch block
// e.printStackTrace();
// }
// }
//
// }, task);
// }
// executor.shutdown();
// try {
// executor.awaitTermination(2, TimeUnit.SECONDS);
// } catch (InterruptedException e) {
// // TODO Auto-generated catch block
// e.printStackTrace();
// }
// for (int i = 0; i < 6; ++i) {
// assertEquals(true, trueValues[i]);
// }
// }
// }

// }
