import io
import os
import configparser
# import urllib.parse
import boto3
import time


print('Loading function') 

s3 = boto3.client('s3')


chunk_size = 200 * 1000000


def lambda_handler(event, context):
    
    # Get the object from the event and show its content type
    # bucket = event['Records'][0]['s3']['bucket']['name']
    # key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    bucket = 'demodayfiles'     
    # key = 'JAIMEP1/2gb.csv'   
    key = 'JAIMEP1/transactions.csv'      
    
    print("Evento recibido del bucket {} y fichero {}".format(bucket,key))    
    
    # print("environment variable: " + os.environ['NUM_LINES'])
    
    
    try:
        
        file = s3.get_object(Bucket=bucket, Key=key)
        
        # print(file['ResponseMetadata']['HTTPHeaders']['content-length'])

        time_ini = time.time()
        num_lines=0
        
        print("Fichero Leido")

        lines = file['Body'].read().splitlines()
        print("Lineas del fichero Leidas")
        size_file = int(file['ResponseMetadata']['HTTPHeaders']['content-length'])
        
        print("El tamaño del archivo es de {} bytes".format(size_file))

        num_lines = len(lines)
        
        print('{:d} lineas leidas en {:0.2f} Seg'.format(num_lines,(time.time()-time_ini)))

        linesbyfile = int(chunk_size / (size_file/num_lines))
        
        print("El número de lineas por archivo es de {:d}".format(linesbyfile))
        print("El número de ficheros a splitear es de {:0.0f}".format(num_lines/linesbyfile))
        
        f = io.StringIO()
        
        #Get times
        time_ini = time.time()
        time_Split = time_ini
        
        cont_lines = 0
        cont_files = 1
        
        for i,line in enumerate(lines):
            
            cont_lines += 1

            numFile = i//linesbyfile

            if cont_lines == linesbyfile:
                time_ini = time.time()
                
                path = "WORK/" + key.split('/')[1] +'%d' % cont_files
                
                # print("Fichero {} con {:d} lineas procesado en {:0.2f} segundos".format(path,cont_lines,(time.time()-time_ini)))
                s3.put_object(Bucket=bucket, Key=path,Body=f.getvalue());
                cont_files += 1
                cont_lines = 0
                
                f.close()
                f = io.StringIO()
        
            f.write(line.decode(encoding='UTF-8')+'\n')
            
        if cont_lines > 0:
            path = "WORK/" + key.split('/')[1] +'%d' % cont_files
            print("Fichero {} con {:d} lineas procesado en {:0.2f} segundos".format(path,cont_lines,(time.time()-time_ini)))

        print('Se han leido {:d} lineas y el proceso total ha durado {} segundos'.format(i,time.time()-time_ini))
        
        s3.put_object(Bucket=bucket, Key=path,Body=f.getvalue());
        f.close()

         

    except Exception as e:
        print("Se ha producido una excepcion ",e)
        print(e)
        
    return
