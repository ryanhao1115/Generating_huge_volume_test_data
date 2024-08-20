import multiprocessing
import random
import string
import csv
import os

def generate_products(start_code, end_code):
    products = []
    for code in range(start_code, end_code):
        pro_name = ''.join(random.choices(string.ascii_uppercase, k=8))
        unit_price = round(random.uniform(0.90, 120.30), 2)
        products.append([f"{code:04d}", pro_name, unit_price])
    return products

def worker(start_code, end_code, queue):
    products = generate_products(start_code, end_code)
    queue.put(products)

def save_to_csv(products, output_file):
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Code", "Pro_name", "Unit_price"])
        for product in products:
            writer.writerow(product)

if __name__ == "__main__":


    # multiprocessing parameters
    num_processes = 4
    product_range = 9999
    chunk_size = product_range // num_processes

   

    manager = multiprocessing.Manager()
    queue = manager.Queue()
    processes = []

    for i in range(num_processes):
        start_code = i * chunk_size + 1
        end_code = (i + 1) * chunk_size + 1 if i < num_processes - 1 else product_range + 1
        p = multiprocessing.Process(target=worker, args=(start_code, end_code, queue))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    # Colleting data from multiprocessing
    all_products = []
    while not queue.empty():
        all_products.extend(queue.get())

    # Save data to csv file
    save_to_csv(all_products, output_file)

    print(f"Product data has been successfully generated and saved to {output_file}")

