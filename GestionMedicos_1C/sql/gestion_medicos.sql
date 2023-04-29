drop table medico cascade constraints;
drop table cliente cascade constraints;
drop table anulacion cascade constraints;
drop table consulta cascade constraints;


drop sequence seq_medico;
drop sequence seq_consulta;
drop sequence seq_anulacion;


create table cliente(
	NIF	varchar(9) primary key,
	nombre	varchar(20) not null,
	ape1	varchar(20) not null,
	ape2	varchar(20) not null,
	direccion varchar(40) 
);

create sequence seq_medico;
create table medico(
	id_medico integer primary key,
	NIF	varchar(9) not null,
	nombre	varchar(20) not null,
	ape1	varchar(20) not null,
	ape2	varchar(20) not null,
	especialidad varchar(20) not null,
	consultas int not null check (consultas >= 0)
);

create sequence seq_consulta;
create table consulta (
	id_consulta	integer primary key,
	fecha_consulta	date not null,
	id_medico integer not null references medico,
	NIF varchar(9) not null references cliente
);

create sequence seq_anulacion;
create table anulacion (
	id_anulacion	integer primary key,
	id_consulta integer not null references consulta,
	fecha_anulacion date not null,
	motivo_anulacion varchar(100) not null
);



create or replace procedure reset_seq( p_seq_name varchar ) is
--From https://stackoverflow.com/questions/51470/how-do-i-reset-a-sequence-in-oracle
    l_val number;
begin
    --Averiguo cual es el siguiente valor y lo guardo en l_val
    execute immediate
    'select ' || p_seq_name || '.nextval from dual' INTO l_val;

    --Utilizo ese valor en negativo para poner la secuencia cero, pimero cambiando el incremento de la secuencia
    execute immediate
    'alter sequence ' || p_seq_name || ' increment by -' || l_val || 
                                                          ' minvalue 0';
   --segundo pidiendo el siguiente valor
    execute immediate
    'select ' || p_seq_name || '.nextval from dual' INTO l_val;

    --restauro el incremento de la secuencia a 1
    execute immediate
    'alter sequence ' || p_seq_name || ' increment by 1 minvalue 0';

end;
/

create or replace procedure inicializa_test is
begin
  reset_seq( 'seq_medico' );
  reset_seq( 'seq_consulta' );
  reset_seq( 'seq_anulacion' );
  
 
        
	delete from anulacion;
	delete from consulta;
	delete from cliente;
	delete from medico;
   
		
    insert into cliente values ('12345678A', 'Juan', 'Garcia', 'Porras', 'C/La Soledad n1');
    insert into cliente values ('87654321B', 'Rosa', 'Ramos', 'Benito', 'C/Estafeta n27');
	insert into cliente values ('78677433R', 'Ana Maria', 'Martin', 'Boyecro', 'Av/Burgos n23');
	
	insert into medico values (seq_medico.nextval, '222222B', 'Jose', 'Sanchez', 'Sabchez', 'Medicina General', 0);
    insert into medico values (seq_medico.nextval,'8766788Y', 'Alejandra', 'Amos', 'Garcia', 'Oncologia', 1);
    

	insert into consulta values (seq_consulta.nextval, to_date('24/03/2023', 'DD/MM/YYYY'), 1, '12345678A');
	insert into consulta values (seq_consulta.nextval, to_date( '25/03/2022', 'DD/MM/YYYY'), 2, '87654321B');


	insert into anulacion values (seq_anulacion.nextval, 1 , to_date('24/02/2023', 'DD/MM/YYYY'), 'Enfermedad infecciosa');
    
	
    commit;
end;
/

exit;