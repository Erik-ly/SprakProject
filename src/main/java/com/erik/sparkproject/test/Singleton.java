package com.erik.sparkproject.test;

/**
 * 单例模式
 * @author Erik
 *
 *在整个程序运行期间，只有一个实例
 *任何外界代码，都不能随意创建实例
 *
 *实现单例模式要点：
 *1.如果不想让外界可以随意创建实例，那么类的构造就必须用private修饰，必须是私有的
 *2.既然类构造方法被私有化了，外界代码要想获取类的实例，不能够随意地去创建，
 *那么只能通过调用类的静态方法，去获取实例
 *3.所以类必须有一个静态方法，getInstance（）来提供获取唯一实例的功能
 *getInstance（）方法必须保证类的实例创建，且仅创建一次，返回一个唯一的实例
 */
public class Singleton {
	
	//首先必须有一个私有的静态变量来引用自己即将被创建出来的单例
	private static Singleton instance = null;
	
	//其次，必须对自己的构建方法使用private进行私有化
	private Singleton() {
		
	}
	
	//最后，需要有一个共有的，静态方法
	//这个方法，负责创建唯一的实例，并且返回这个唯一的实例
	//必须考虑到多线程并发访问安全的控制
	public static Singleton getInstance() {
		//两步检查机制
		//首先第一步，多个线程过来的时候，判断instance是否为null
		//如果为null再往下走
		if (instance == null) {
			//这里进行多线程的同步
			//同一时间，只有一个线程获取到Singleton Class对象的锁进行后续的代码
			//其他的线程，都只能在原地等待，获取锁
			synchronized(Singleton.class) {
				//只有第一个获取到锁的线程，进入到这里会发现instance是null
				//然后才会去创建这个单例
				//此后，线程哪怕是走到了这一步，也会发现instance已经不是null了
				//就不会反复创建一个单例
				if (instance == null) {
					instance = new Singleton();
				}
			}
		}
		return instance;
	}
	
}
