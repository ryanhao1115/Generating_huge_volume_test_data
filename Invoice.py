import multiprocessing
import random
import csv
import os

def generate_invoices(start_code, end_code, product_range, customer_range):
    invoices = []
    for invoice_code in range(start_code, end_code):
        product_code = f"{random.randint(1, product_range):04d}"
        customer_code = f"{random.randint(101, customer_range):07d}"
        qty = random.randint(1, 1000)
        invoices.append([f"{invoice_code:012d}", product_code, customer_code, qty])
    return invoices

def worker(start_code, end_code, product_range, customer_range, queue):
    invoices = generate_invoices(start_code, end_code, product_range, customer_range)
    queue.put(invoices)

def save_to_csv(invoices, output_file):
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Invoice_code", "Product_code", "Customer_code", "Qty"])
        for invoice in invoices:
            writer.writerow(invoice)

if __name__ == "__main__":
 
    
    num_processes = 16
    invoice_range = 1100000000
    chunk_size = invoice_range // num_processes

    product_range = 9999
    customer_range = 3000005



    manager = multiprocessing.Manager()
    queue = manager.Queue()
    processes = []

    for i in range(num_processes):
        start_code = i * chunk_size + 1
        end_code = (i + 1) * chunk_size + 1 if i < num_processes - 1 else invoice_range + 1
        p = multiprocessing.Process(target=worker, args=(start_code, end_code, product_range, customer_range, queue))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()


    all_invoices = []
    while not queue.empty():
        all_invoices.extend(queue.get())


    save_to_csv(all_invoices, output_file)

    print(f"Invoice data has been successfully generated and saved to {output_file}")

