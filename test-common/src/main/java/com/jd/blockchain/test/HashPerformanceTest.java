package com.jd.blockchain.test;

import java.util.Random;

import com.jd.blockchain.utils.hash.MurmurHash3;
import com.jd.blockchain.utils.security.RipeMD160Utils;
import com.jd.blockchain.utils.security.ShaUtils;

public class HashPerformanceTest {
	
	

	public static void main(String[] args) {
		
		final long flag = 0x1111 << 60;

		byte[] murmurHash = new byte[128];
		HashFunction murmur3 = new HashFunction() {
			
			@Override
			public byte[] hash(byte[] data) {
				long hash = MurmurHash3.murmurhash3_x64_64_1(data, 0, data.length, 1024);
				byte id = (byte) (hash & 0xFF);
				return null;
			}
			
			@Override
			public Object getName() {
				return "Murmur3";
			}
		};
		HashFunction sha256 = new HashFunction() {
			
			@Override
			public byte[] hash(byte[] data) {
				return ShaUtils.hash_256(data);
			}
			
			@Override
			public Object getName() {
				return "Sha256";
			}
		};
		HashFunction ripleMD160 = new HashFunction() {
			
			@Override
			public byte[] hash(byte[] data) {
				return RipeMD160Utils.hash(data);
			}
			
			@Override
			public Object getName() {
				return "RIPEMD160";
			}
		};
		
		test(murmur3);
		test(ripleMD160);
		test(sha256);
	}
	
	
	private static void test(HashFunction hashFunc) {
		long count = 1000000;
		
		byte[] bytes = new byte[64];
		
		Random rand = new Random();
		rand.nextBytes(bytes);
		
		long startTs = System.currentTimeMillis();
		for (int i = 0; i < count; i++) {
			hashFunc.hash(bytes);
		}
		
		long endTs = System.currentTimeMillis();
		
		long tps = count * 1000 / (endTs - startTs);
		System.out.printf("Algorithm=%s, TPS=%s \r\n", hashFunc.getName(), tps);
	}
	
	public static interface HashFunction{
		
		byte[] hash(byte[] data);

		Object getName();
		
	}

}
