#!/usr/bin/env python3
"""
Simple hashgen implementation for CS553 HW5
Generates Blake3 hashes and sorts them
"""

import blake3
import struct
import os
import time
import argparse
from multiprocessing import Pool, cpu_count
import random

def generate_hash_batch(args):
    """Generate a batch of hashes"""
    batch_size, seed = args
    random.seed(seed)
    records = []
    
    for _ in range(batch_size):
        # Generate 6-byte random nonce
        nonce = bytes([random.randint(0, 255) for _ in range(6)])
        
        # Generate Blake3 hash (10 bytes)
        hasher = blake3.blake3()
        hasher.update(nonce)
        hash_bytes = hasher.digest(length=10)
        
        # Create 16-byte record: 10-byte hash + 6-byte nonce
        record = hash_bytes + nonce
        records.append(record)
    
    return records

def hashgen(k, output_file, num_threads=None):
    """
    Generate 2^k hashes and sort them
    
    Args:
        k: Power of 2 for number of records
        output_file: Output file path
        num_threads: Number of threads (default: CPU count)
    """
    if num_threads is None:
        num_threads = cpu_count()
    
    total_records = 1 << k
    batch_size = max(1000, total_records // (num_threads * 100))
    
    print(f"Generating {total_records:,} hashes with {num_threads} threads")
    print(f"Batch size: {batch_size:,}")
    
    start_time = time.time()
    
    # Generate batches
    num_batches = (total_records + batch_size - 1) // batch_size
    tasks = [(min(batch_size, total_records - i * batch_size), 
              random.randint(0, 2**32)) for i in range(num_batches)]
    
    # Generate hashes in parallel
    all_records = []
    with Pool(num_threads) as pool:
        for batch_records in pool.imap_unordered(generate_hash_batch, tasks):
            all_records.extend(batch_records)
            if len(all_records) % 1000000 == 0:
                print(f"Generated {len(all_records):,} records...")
    
    gen_time = time.time()
    print(f"Generation time: {gen_time - start_time:.2f} seconds")
    
    # Sort records
    print("Sorting records...")
    all_records.sort()
    sort_time = time.time()
    print(f"Sort time: {sort_time - gen_time:.2f} seconds")
    
    # Write to file
    print(f"Writing to {output_file}...")
    with open(output_file, 'wb') as f:
        for record in all_records:
            f.write(record)
    
    write_time = time.time()
    print(f"Write time: {write_time - sort_time:.2f} seconds")
    
    total_time = write_time - start_time
    print(f"\nTotal time: {total_time:.2f} seconds")
    print(f"File size: {os.path.getsize(output_file) / (1024**3):.2f} GB")
    
    return total_time

def verify_sorted(filename):
    """Verify that a file is sorted"""
    print(f"Verifying {filename}...")
    
    with open(filename, 'rb') as f:
        prev = None
        count = 0
        while True:
            record = f.read(16)
            if not record:
                break
            
            if prev and record < prev:
                print(f"ERROR: File not sorted at record {count}")
                return False
            
            prev = record
            count += 1
            
            if count % 1000000 == 0:
                print(f"Verified {count:,} records...")
    
    print(f"SUCCESS: File is sorted ({count:,} records)")
    return True

def search(filename, k, num_searches=10, difficulty=3):
    """
    Search for hashes in the sorted file
    
    Args:
        filename: Input file path
        k: Power of 2 for number of records
        num_searches: Number of searches to perform
        difficulty: Number of bytes to match (3 or 4)
    """
    total_records = 1 << k
    record_size = 16
    
    print(f"Performing {num_searches} searches with difficulty {difficulty}")
    
    start_time = time.time()
    found = 0
    total_comps = 0
    
    for i in range(num_searches):
        # Generate random search query
        query = bytes([random.randint(0, 255) for _ in range(difficulty)])
        
        # Binary search
        left, right = 0, total_records - 1
        comps = 0
        matches = []
        
        with open(filename, 'rb') as f:
            while left <= right:
                mid = (left + right) // 2
                f.seek(mid * record_size)
                record = f.read(record_size)
                comps += 1
                
                if record[:difficulty] == query:
                    matches.append(record)
                    found += 1
                    break
                elif record[:difficulty] < query:
                    left = mid + 1
                else:
                    right = mid - 1
        
        total_comps += comps
        
        if matches:
            print(f"[{i}] {query.hex()} MATCH (comps={comps})")
        else:
            print(f"[{i}] {query.hex()} NOTFOUND (comps={comps})")
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"\nSearch Summary:")
    print(f"  Total searches: {num_searches}")
    print(f"  Found: {found}")
    print(f"  Not found: {num_searches - found}")
    print(f"  Total time: {total_time:.6f} s")
    print(f"  Avg time: {(total_time / num_searches) * 1000:.3f} ms")
    print(f"  Throughput: {num_searches / total_time:.2f} searches/sec")
    print(f"  Total comparisons: {total_comps}")
    print(f"  Avg comparisons: {total_comps / num_searches:.2f}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Hash generator and sorter')
    parser.add_argument('-k', type=int, required=True, help='Power of 2 for number of records')
    parser.add_argument('-f', '--file', required=True, help='Output file')
    parser.add_argument('-t', '--threads', type=int, help='Number of threads')
    parser.add_argument('-v', '--verify', action='store_true', help='Verify sorted file')
    parser.add_argument('-s', '--search', type=int, help='Number of searches')
    parser.add_argument('-q', '--difficulty', type=int, default=3, help='Search difficulty (3 or 4)')
    
    args = parser.parse_args()
    
    if args.verify:
        verify_sorted(args.file)
    elif args.search:
        search(args.file, args.k, args.search, args.difficulty)
    else:
        hashgen(args.k, args.file, args.threads)