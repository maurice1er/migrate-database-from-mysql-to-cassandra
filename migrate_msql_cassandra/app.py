import mysql.connector as mysql
from cassandra.cluster import Cluster

mydb = mysql.connect(
  	host="localhost",
  	user="yerd",
  	passwd="XXXXXXXXXXXXXX",
  	database="navetane2"
)

cluster = Cluster()
session = cluster.connect() 
session.execute("CREATE KEYSPACE IF NOT EXISTS navetane2 WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}")


mycursor = mydb.cursor()
session.execute('USE navetane2')



####################################################################################
#########################    DELETE TABLE IF EXISTS       ##########################
####################################################################################

def delete_liste_equipes():
	sql = """
	TRUNCATE liste_equipes
	"""
	session.execute(sql)
delete_liste_equipes()


def delete_liste_matchs():
	sql = """
	TRUNCATE liste_matchs
	"""
	session.execute(sql)
delete_liste_matchs()


def delete_liste_stades():
	sql = """
	TRUNCATE liste_stades
	"""
	session.execute(sql)
delete_liste_stades()


def delete_liste_arbitres():
	sql = """
	TRUNCATE liste_arbitres
	"""
	session.execute(sql)
delete_liste_arbitres()


def delete_liste_participants_match():
	sql = """
	TRUNCATE liste_participants_match
	"""
	session.execute(sql)
delete_liste_participants_match()


def delete_joueur_jouer_match():
	sql = """
	TRUNCATE liste_joueur_jouer_match
	"""
	session.execute(sql)
delete_joueur_jouer_match()


def delete_liste_score_match():
	sql = """
	TRUNCATE liste_score_match
	"""
	session.execute(sql)
delete_liste_score_match()


def delete_liste_equipes_poule():
	sql = """
	TRUNCATE liste_equipes_poule
	"""
	session.execute(sql)
delete_liste_equipes_poule()


def delete_liste_equipes_poule_b():
	sql = """
	TRUNCATE liste_equipes_poule_b
	"""
	session.execute(sql)
delete_liste_equipes_poule_b()


def delete_liste_finalistes():
	sql = """
	TRUNCATE liste_finalistes
	"""
	session.execute(sql)
delete_liste_finalistes()


####################################################################################
########################    FIN DELETE TABLE IF EXISTS     #########################
####################################################################################






####################################################################################
##############################    CREATE  TABLE        #############################
####################################################################################


def create_liste_equipes():
	sql = """
	CREATE TABLE IF NOT EXISTS liste_equipes(id_equipe int primary key,nom_equipe varchar,adresse_equipe varchar)
	"""
	session.execute(sql)
	return True
create_liste_equipes()


def create_liste_matchs():
	sql = """
	CREATE TABLE IF NOT EXISTS liste_matchs(id_matchs int primary key,date_matchs date,debut_matchs time,fin_matchs time,nom_equipe1 varchar,nom_equipe2 varchar,libelle_tour varchar, nom_stade varchar,capacite int,adresse_stade varchar)
	"""
	session.execute(sql)
	return True
create_liste_matchs()


def create_liste_stades():
	sql = """
	CREATE TABLE IF NOT EXISTS liste_stades(id_stade int primary key,nom_stade varchar,capacite int,adresse_stade varchar)
	"""
	session.execute(sql)
	return True
create_liste_stades()


def create_liste_arbitres():
	sql = """
	CREATE TABLE IF NOT EXISTS liste_arbitres(id_arbitre int primary key,nom_arbitre varchar,tel_arbitre varchar)
	"""
	session.execute(sql)
	return True
create_liste_arbitres()


def create_liste_participants_match():
	sql = """
	CREATE TABLE IF NOT EXISTS liste_participants_match(id int primary key, num_licence varchar, nom_joueur varchar, poste_joueur varchar, commentaire varchar, libelle_tour varchar)
	"""
	session.execute(sql)
	return True
create_liste_participants_match()


def create_joueur_jouer_match():
	sql = """
	CREATE TABLE IF NOT EXISTS liste_joueur_jouer_match(id int primary key,nom_joueur varchar, commentaire varchar)
	"""
	session.execute(sql)
	return True
create_joueur_jouer_match()


def create_liste_score_match():
	sql = """
	CREATE TABLE IF NOT EXISTS liste_score_match(id int primary key, nom_equipe1 varchar,nom_equipe2 varchar,score varchar)
	"""
	session.execute(sql)
	return True
create_liste_score_match()


def create_liste_equipes_poule():
	sql = """
	CREATE TABLE IF NOT EXISTS liste_equipes_poule(id int primary key, nom_equipe varchar, libelle_poule varchar)
	"""
	session.execute(sql)
	return True
create_liste_equipes_poule()


def create_liste_equipes_poule_b():
	sql = """
	CREATE TABLE IF NOT EXISTS liste_equipes_poule_b(id int primary key,nom_equipe1 varchar,nom_equipe2 varchar,libelle_tour varchar)
	"""
	session.execute(sql)
	return True
create_liste_equipes_poule_b()


def create_liste_finalistes():
	sql = """
	CREATE TABLE IF NOT EXISTS liste_finalistes(id_matchs int primary key, nom_equipe1 varchar, nom_equipe2 varchar)
	"""
	session.execute(sql)
	return True
create_liste_finalistes()


####################################################################################
#############################      FIN CREATE TABLE       ##########################
####################################################################################






#######################################################################################
#############################        INSERT           #################################
#######################################################################################

def insert_equipes(id_equipe,nom_equipe,adresse_equipe):
	sql = """
	INSERT INTO liste_equipes(id_equipe,nom_equipe,adresse_equipe) VALUES({},'{}','{}')
	"""
	session.execute(sql.format(id_equipe,nom_equipe,adresse_equipe))
	return True

def insert_matchs(id_matchs,date_matchs,debut_matchs,fin_matchs,nom_equipe1,nom_equipe2,libelle_tour, nom_stade,capacite,adresse_stade):
	sql = """
	INSERT INTO liste_matchs(id_matchs,date_matchs,debut_matchs,fin_matchs,nom_equipe1,nom_equipe2,libelle_tour, nom_stade,capacite,adresse_stade) VALUES({},'{}','{}','{}','{}','{}','{}','{}',{},'{}')
	"""
	session.execute(sql.format(id_matchs,date_matchs,debut_matchs,fin_matchs,nom_equipe1,nom_equipe2,libelle_tour, nom_stade,capacite,adresse_stade))
	return True

def insert_stades(id_stade, nom_stade, capacite, adresse_stade):
	sql = """
	INSERT INTO liste_stades(id_stade, nom_stade, capacite, adresse_stade) VALUES({},'{}',{},'{}')
	"""
	session.execute(sql.format(id_stade, nom_stade, capacite, adresse_stade))
	return True

def insert_arbitres(id_arbitre, nom_arbitre, tel_arbitre):
	sql = """
	INSERT INTO liste_arbitres(id_arbitre, nom_arbitre, tel_arbitre) VALUES({},'{}','{}')
	"""
	session.execute(sql.format(id_arbitre, nom_arbitre, tel_arbitre))
	return True


def insert_participants_match(id, num_licence, nom_joueur, poste_joueur, commentaire, libelle_tour):
	sql = """
	INSERT INTO liste_participants_match(id, num_licence, nom_joueur, poste_joueur, commentaire, libelle_tour) VALUES({},'{}','{}','{}','{}','{}')
	"""
	session.execute(sql.format(id, num_licence, nom_joueur, poste_joueur, commentaire, libelle_tour))
	return True

	

def insert_joueur_jouer_match(id, nom_joueur, commentaire):
	sql = """
	INSERT INTO liste_joueur_jouer_match(id, nom_joueur, commentaire) VALUES({},'{}','{}')
	"""
	session.execute(sql.format(id, nom_joueur, commentaire))
	return True


def insert_liste_score_match(id, nom_equipe1, nom_equipe2, score):
	sql = """
	INSERT INTO liste_score_match(id, nom_equipe1, nom_equipe2, score) VALUES({},'{}','{}','{}')
	"""
	session.execute(sql.format(id, nom_equipe1, nom_equipe2, score))
	return True
	

def insert_liste_equipes_poule(id, nom_equipe, libelle_poule):
	sql = """
	INSERT INTO liste_equipes_poule(id, nom_equipe, libelle_poule) VALUES({},'{}','{}')
	"""
	session.execute(sql.format(id, nom_equipe, libelle_poule))
	return True


def insert_liste_equipes_poule_b(id, nom_equipe1, nom_equipe2, libelle_tour):
	sql = """
	INSERT INTO liste_equipes_poule_b(id, nom_equipe1, nom_equipe2, libelle_tour) VALUES({},'{}','{}','{}')
	"""
	session.execute(sql.format(id, nom_equipe1, nom_equipe2, libelle_tour))
	return True


def insert_liste_finalistes(id_matchs, nom_equipe1, nom_equipe2):
	sql = """
	INSERT INTO liste_finalistes(id_matchs, nom_equipe1, nom_equipe2) VALUES({},'{}','{}')
	"""
	session.execute(sql.format(id_matchs, nom_equipe1, nom_equipe2))
	return True



#######################################################################################
#############################       FIN INSERT           ##############################
#######################################################################################





#######################################################################################
#############################        MIGRATION           ##############################
#######################################################################################


def liste_equipes():
	sql = """
	SELECT id_equipe, nom_equipe, adresse_equipe FROM equipe
	"""
	mycursor.execute(sql)
	query = mycursor.fetchall()

	for x in query:
		insert_equipes(x[0], x[1], x[2])
	print(mycursor.rowcount,"observations | liste_equipes [#",end="")
	print("##############] 100%")

	return True
liste_equipes()


def liste_matchs():
	sql = """
	SELECT matchs.id_matchs,matchs.date_matchs,matchs.debut_matchs,matchs.fin_matchs,ea.nom_equipe,eb.nom_equipe,tour.libelle_tour, stade.nom_stade,stade.capacite,stade.adresse_stade FROM matchs, equipe ea,equipe eb,stade,tour,disputer WHERE matchs.id_matchs=disputer.id_matchs AND disputer.id_equipe=ea.id_equipe AND disputer.id_equipe_disputer=eb.id_equipe AND stade.id_stade=matchs.id_stade AND tour.id_tour=matchs.id_tour ORDER BY matchs.id_matchs
	"""
	mycursor.execute(sql)
	query = mycursor.fetchall()

	for x in query:
		insert_matchs(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9])
	print(mycursor.rowcount, " observations | liste_matchs [#",end="")
	print("##############] 100%")

	return True
liste_matchs()


def liste_stades():
	sql = """
	SELECT * FROM stade
	"""
	mycursor.execute(sql)
	query = mycursor.fetchall()

	for x in query:
		insert_stades(x[0],x[1],x[2],x[3])
	print(mycursor.rowcount, " observations | liste_stades [#",end="")
	print("##############] 100%")

	return True
liste_stades()


def liste_arbitres():
	sql = """
	SELECT * FROM arbitre
	"""
	mycursor.execute(sql)
	query = mycursor.fetchall()

	for x in query:
		insert_arbitres(x[0],x[1],x[2])
	print(mycursor.rowcount, " observations | liste_arbitres [#",end="")
	print("##############] 100%")
	
	return True
liste_arbitres()


def liste_participants_match():
	sql = """
	SELECT joueur.id_joueur,licence.num_licence,joueur.nom_joueur,joueur.poste_joueur,jouer.commentaire, tour.libelle_tour FROM jouer,licence,joueur,matchs,tour WHERE licence.id_licence=jouer.id_licence AND licence.id_joueur=joueur.id_joueur AND tour.id_tour=matchs.id_tour AND jouer.id_matchs=matchs.id_matchs ORDER BY joueur.id_joueur
	"""
	mycursor.execute(sql)
	query = mycursor.fetchall()

	for x in query:
		insert_participants_match(x[0],x[1],x[2],x[3],x[4],x[5])
	print(mycursor.rowcount, " observations | liste_participants_match [#",end="")
	print("##############] 100%")

	return True
liste_participants_match()


def liste_joueur_jouer_match():
	sql = """
	SELECT joueur.id_joueur,joueur.nom_joueur,jouer.commentaire FROM jouer,licence,matchs,joueur WHERE jouer.id_licence=licence.id_licence AND licence.id_joueur=joueur.id_joueur AND jouer.id_matchs=matchs.id_matchs
	"""
	# AND matchs.id_matchs= {} AND jouer.commentaire LIKE {}
	mycursor.execute(sql)
	query = mycursor.fetchall()

	for x in query:
		insert_joueur_jouer_match(x[0],x[1],x[2])
	print(mycursor.rowcount, " observations | liste_joueur_jouer_match [#",end="")
	print("##############] 100%")
	
	return True
liste_joueur_jouer_match()
	

def liste_score_match():
	sql = """
	SELECT matchs.id_matchs, ea.nom_equipe,eb.nom_equipe,score FROM equipe ea, equipe eb, disputer, matchs WHERE ea.id_equipe=disputer.id_equipe AND eb.id_equipe=disputer.id_equipe_disputer AND matchs.id_matchs=disputer.id_matchs ORDER BY matchs.id_matchs
	"""
	mycursor.execute(sql)
	query = mycursor.fetchall()

	for x in query:
		insert_liste_score_match(x[0],x[1],x[2],x[3])
	print(mycursor.rowcount, " observations | liste_score_match [#",end="")
	print("##############] 100%")

	return True
liste_score_match()


def liste_equipes_poule():
	sql = """SELECT equipe.id_equipe,equipe.nom_equipe,poule.libelle_poule FROM contenir,equipe,poule WHERE poule.id_poule=contenir.id_poule AND contenir.id_equipe=equipe.id_equipe"""
	mycursor.execute(sql)
	query = mycursor.fetchall()
	
	for x in query:
		insert_liste_equipes_poule(x[0],x[1],x[2])
	print(mycursor.rowcount, " observations | liste_equipes_poule [#",end="")
	print("##############] 100%")
	
	return True
liste_equipes_poule()


def liste_equipes_poule_b():
	sql = """
	SELECT eb.id_equipe,ea.nom_equipe,eb.nom_equipe,tour.libelle_tour FROM matchs,equipe ea,equipe eb,tour,disputer WHERE tour.id_tour=matchs.id_tour AND disputer.id_equipe=ea.id_equipe AND disputer.id_equipe_disputer=eb.id_equipe AND disputer.id_matchs=matchs.id_matchs AND tour.libelle_tour="Quart-final"
	"""
	mycursor.execute(sql)
	query = mycursor.fetchall()
	
	for x in query:
		insert_liste_equipes_poule_b(x[0],x[1],x[2],x[3])
	print(mycursor.rowcount, " observations | liste_equipes_poule_b [#",end="")
	print("##############] 100%")
	
	return True
liste_equipes_poule_b()


def liste_finalistes():
	sql = """
	SELECT matchs.id_matchs, ea.nom_equipe,eb.nom_equipe FROM equipe ea, equipe eb, disputer, matchs WHERE ea.id_equipe=disputer.id_equipe AND eb.id_equipe=disputer.id_equipe_disputer AND matchs.id_matchs=disputer.id_matchs AND matchs.id_matchs=32 
	"""
	mycursor.execute(sql)
	query = mycursor.fetchall()

	for x in query:
		insert_liste_finalistes(x[0],x[1],x[2])
	print(mycursor.rowcount, " observations | liste_finalistes [#",end="")
	print("##############] 100%")
	
	return True
liste_finalistes()

print("",end="\n")
for x in range(1,20):
	print("#"*x)

#######################################################################################
#############################      FIN  MIGRATION           ###########################
#######################################################################################


# def menu():
# 	l = """
# 	1- Liste des equipes
# 	2- Liste des matchs
# 	3- Liste des stades
# 	4- Liste des arbitres
# 	5- Liste des joueurs participants a un match
# 	6- Liste des joueurs ayant joues pour un match donne
# 	7- Liste du score d un match donne
# 	8- Liste des equipes de chaque poule
# 	9- Liste des equipes qui passent en poule B
# 	10- Liste des equipes finalistes
# 	0- Quitter
# 	"""

	# print(l)

	# entree = input("Faites un choix")

	# if entree == "1":
	# 	print(liste_equipes())
	# elif entree == "2":
	# 	print(liste_matchs())
	# elif entree == "3":
	# 	print(liste_stades())
	# elif entree == "4":
	# 	print(liste_arbitres())
	# elif entree == "5":
	# 	print(liste_participants_match())
	# elif entree == "6":
	# 	print(liste_joueur_jouer_match())
	# elif entree == "7":
	# 	print(liste_score_match())
	# elif entree == "8":
	# 	print(liste_equipes_poule())
	# elif entree == "9":
	# 	print(liste_equipes_poule_b())
	# elif entree == "10":
	# 	print(liste_finalistes())
	# elif entree == "0":
	# 	print("Byeeeeeeeeeeeeee!")
	# else:
	# 	print("Erroooooooooooor!")

#menu()

