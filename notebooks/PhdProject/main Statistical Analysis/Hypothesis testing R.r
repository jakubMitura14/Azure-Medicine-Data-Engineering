# Databricks notebook source
# MAGIC %md
# MAGIC #libraries and imported from data related to the SABHA correction method

# COMMAND ----------

# this file provides functions for computing q-hat for the SABHA method
# includes:
# Solve_q_step = q-hat of the form (eps,...,eps,1,...,1)
# Solve_q_ordered = q-hat of the form (q1,...,qn) with eps<=q1<=q2<=...<=qn<=1
# Solve_q_block = q-hat that is block-wise constant, and eps<=q<=1
# Solve_q_TV_1dim = q-hat that has bounded 1d total variation norm, and eps<=q<=1
# Solve_q_TV = q-hat that has bounded total variation norm on a specified graph, and eps<=q<=1

##########################################################
## SABHA with ordered q: q = step function
##########################################################

# Solve_q_step: returns a vector q subject to the constraint that (q1, q2, ...,  qn) = (eps, eps, ..., eps, 1, 1, ..., 1), with as many eps's as possible, subject to sum_i 1{P[i]>tau}/q[i](1-tau) <= n
Solve_q_step = function(Pvals, tau, eps){
  n = length(Pvals)
  sum_p_over_threshold = sum(Pvals > tau)
  K = max(which(cumsum(Pvals > tau) <= (n*(1-tau) - sum_p_over_threshold) / (1/eps - 1)), 0)
  
  return (c(rep(eps, K), rep(1, n-K)))
}

##########################################################
## SABHA with ordered q: q satisfies eps<=q1<=q2<=...<=qn=1
##########################################################

Solve_q_ordered = function(Pvals, tau, eps, ADMM_params){
	PAVA_alg = create_PAVA_alg_function()
	M = diag(length(Pvals))
	q = Solve_q_ADMM(Pvals, tau, eps, M, PAVA_alg, ADMM_params)
	q
}

create_PAVA_alg_function = function(){
	function(y){
         # solving: min{1/2 ||x-y||^2_2 : x_1<=..<=x_n}
	       # PAVA algorithm (Barlow et al 1972)
	       n=length(y)
	       thresh = 1e-8
	       groups = 1:n
	       block = 1
	
	       stop = FALSE
	       while(!stop){
		         if(any(groups==block+1)){
			             block_plus = which(groups==block+1)
			             if(mean(y[which(groups==block)])<=mean(y[which(groups==block+1)])+thresh){
				              block = block+1
			             } else{
				              groups[which(groups>block)] = groups[which(groups>block)] - 1
				              stop_inner = FALSE
				              while(!stop_inner)
					               if(any(groups==block-1)){
						               if(mean(y[which(groups==block-1)])>mean(y[which(groups==block)])+thresh){
							                groups[which(groups>=block)] = groups[which(groups>=block)] - 1
							                block = block-1
						               } else{
							                stop_inner=TRUE
						               }
					               } else{
						                stop_inner=TRUE
					               }
				           }
			       } else{
				        stop=TRUE
			       }
	       }
	       x=y
	       for(i in 1:max(groups)){
		         x[which(groups==i)]=mean(y[which(groups==i)])
	       }
	       x
     }
     
}    
     
     
##########################################################
## SABHA with block q: q = constant over blocks
##########################################################
     
Solve_q_block = function(Pvals, tau, eps, blocks, ADMM_params){
	# blocks[i] gives the index of the block to which Pvals[i] belongs
	block_proj = create_block_function(blocks)
	q_init = block_proj((Pvals>tau)/(1-tau))
	if(min(q_init)>=eps & max(q_init)<=1){
		q = q_init
	}else{
		M = diag(length(Pvals))
		q = Solve_q_ADMM(Pvals, tau, eps, M, block_proj, ADMM_params)
	}
	q
}  
     
create_block_function = function(blocks){
	function(y){
         # solving: min{1/2 ||x-y||^2_2 : x is constant over the blocks}
		x = y
		block_inds = sort(unique(blocks))
		for(i in block_inds){
			x[which(blocks==block_inds[i])] = mean(x[which(blocks==block_inds[i])])
		}
		x
	}
}     
     
##########################################################
## SABHA with TV norm constraint on q: ||q||_TV <= TV_bd
##########################################################

Solve_q_TV_1dim = function(Pvals, tau, eps, TV_bd, ADMM_params){
	edges = cbind(1:(length(Pvals)-1),2:length(Pvals))
	Solve_q_TV(Pvals, tau, eps, edges, TV_bd, ADMM_params)
}

Solve_q_TV_2dim = function(Pvals, tau, eps, TV_bd, ADMM_params){
	n1 = dim(Pvals)[1]
	n2 = dim(Pvals)[2]
	edges = NULL
	get_ind = function(i,j){i+(j-1)*n1}
	# horizontal edges
	for(i in 1:n1){for(j in 1:(n2-1)){edges=rbind(edges,c(get_ind(i,j),get_ind(i,j+1)))}}
	# vertical edges
	for(j in 1:n2){for(i in 1:(n1-1)){edges=rbind(edges,c(get_ind(i,j),get_ind(i+1,j)))}}
	Solve_q_TV(c(Pvals), tau, eps, edges, TV_bd, ADMM_params)
}
     
Solve_q_TV = function(Pvals, tau, eps, edges, TV_bd, ADMM_params){
	# edges is a e-by-2 matrix giving the edges of the adjacency graph
	# edges[i,1:2] gives the indices of the nodes on the i-th edge
	# constraint: sum_{i=1,..,e} |q[edges[i,1]] - q[edges[i,2]]| <= TV_bd
	L1_proj = create_L1_function(TV_bd)
	nedge = dim(edges)[1]; n = length(Pvals)
	M = matrix(0,nedge,n); for(i in 1:nedge){M[i,edges[i,1]]=1; M[i,edges[i,2]]=-1}
	q = Solve_q_ADMM(Pvals, tau, eps, M, L1_proj, ADMM_params)
	q
}  
    
create_L1_function = function(bound){
	function(y){
         # solving: min{1/2 ||x-y||^2_2 : ||x||_1 <= bound}
        if(sum(abs(y))<=bound){x=y} else{
			    mu = sort(abs(y), decreasing = TRUE)
    	    xi = max(which(mu - (cumsum(mu)-bound)/(1:length(mu))>0))
        	theta = (sum(mu[1:xi])-bound)/xi
	        tmp = abs(y)-theta
    	    x = rep(0, length(tmp))
        	x[which(tmp>0)] = tmp[which(tmp>0)]
	        x[which(tmp<=0)] = 0
    	    x = x*sign(y)
    	  }
        x
	}
}        
     
     
##########################################################
## SABHA ADMM algorithm
##########################################################

Solve_q_ADMM = function(Pvals, tau, eps, M, projection, ADMM_params){
# min -sum_i (B[i]*log((1-tau) q[i]) + (1-B[i])*log(1-(1-tau) q[i]))
# subject to (1) q \in Qset (characterized by M*q \in Mset)
# and (2) sum_i B[i]/q[i] <= gamma and (3) eps<=q<=1
# introduce auxiliary variables x, y under the constraint Mq = x, q = y
# ADMM optimization:
# minimize -sum_i (B_i*log((1-tau) q_i)+(1-B_i)*log(1-(1-tau) q_i)) + <u, Mq-x> + <v, q-y> + alpha/2 ||Mq-x||^2 + beta/2 ||q-y||^2 + alpha/2 (q-qt)'(eta I - M'M)(q-qt)
# where qt is the previous iteration's q value
  
# ADMM_params are: alpha, beta, eta, max_iters, converge_thr
	alpha_ADMM = ADMM_params[1]
	beta = ADMM_params[2]
	eta = ADMM_params[3]
	max_iters = ADMM_params[4]
	converge_thr = ADMM_params[5]

	n = length(Pvals)
	B = (Pvals > tau) 
  gamma = n*(1-tau) # bound on sum_i (Pvals[i]>tau) / q[i]*(1-tau)
	q = y = rep(1,n)
	v = rep(0,n)
	u = x = rep(0,dim(M)[1])
	
	converge_check = function(q,x,y,u,v,q_old,x_old,y_old,u_old,v_old){
		max(c(sqrt(sum((q-q_old)^2))/sqrt(1+sum(q_old^2)),
          sqrt(sum((x-x_old)^2))/sqrt(1+sum(x_old^2)),
          sqrt(sum((y-y_old)^2))/sqrt(1+sum(y_old^2)),
          sqrt(sum((u-u_old)^2))/sqrt(1+sum(u_old^2)),
          sqrt(sum((v-v_old)^2))/sqrt(1+sum(v_old^2))))
	}
	
	stop = FALSE
	iter = 0
  while(!stop){
    iter = iter+1
    q_old = q; x_old = x; y_old = y; u_old = u; v_old = v
    q = q_update(B, M, tau,eps,q,x,y,u,v,alpha_ADMM,gamma,beta, eta)
    x = x_update(B, M, tau,eps,q,x,y,u,v,alpha_ADMM,gamma,beta, eta, projection)
    y = y_update(B, M, tau,eps,q,x,y,u,v,alpha_ADMM,gamma,beta, eta)
    u = u_update(B, M, tau,eps,q,x,y,u,v,alpha_ADMM,gamma,beta, eta)
	  v = v_update(B, M, tau,eps,q,x,y,u,v,alpha_ADMM,gamma,beta, eta)
	  if(converge_check(q,x,y,u,v,q_old,x_old,y_old,u_old,v_old)<=converge_thr){stop=TRUE}
	  if(iter>=max_iters){stop=TRUE}
  }
    
  return(q)
    
}


# inverse_sum_prox solves: min{1/2 ||x-y||^2 : x_i>0, sum_i 1/x_i <= bound}
# Used in y-update step of ADMM
inverse_sum_prox = function(y,bound){

	y = pmax(0,y) # the solution will have all positive x_i's now
					# and we can now ignore the constraint x_i>0
	
	if(sum(1/y)<= bound){
		x=y
	}else{ # use Lagrange multipliers
		
		# we should have - lambda * d/dx_j (sum_i 1/x_i) = d/dx_j (1/2 ||x-y||^2)
		# for all j, for some single lambda>0
		# in other words, lambda / x^2 = x-y (this holds elementwise)
		# rearranging, lambda = x^3 - x^2*y
		# let c = log(lambda) so that it's real-valued
		# we need to solve x^3 - x^2*y - exp(c) = 0 (elementwise)
		
		cuberoot = function(c){ # this solves the cubic equation x^3-x^2*y-exp(c)=0
			temp1 = ((y/3)^3 + exp(c)/2 + (exp(c)*(y/3)^3 + exp(c)^2/4)^0.5)
			temp2 = ((y/3)^3 + exp(c)/2 - (exp(c)*(y/3)^3 + exp(c)^2/4)^0.5)
			x = sign(temp1)*abs(temp1)^(1/3) + sign(temp2)*abs(temp2)^(1/3) + (y/3)
			x
		}
		
		# now we need to choose c, i.e. choose the lagrange multiplier lambda=exp(c)
		# the right value of c is the one that produces an x satisfying sum_i 1/x_i = bound
		
		c = uniroot(function(c){sum(1/cuberoot(c))-bound},c(-100,100))$root
		x = cuberoot(c)
	}
	x
}

q_update = function(B, M, tau,eps,q,x,y,u,v,alpha,gamma,beta, eta){
# minimize -sum_i (B_i*log((1-tau) q_i)+(1-B_i)*log(1-(1-tau) q_i)) + <u, Mq-x> + <v, q-y> + alpha/2 ||Mq-x||^2 + beta/2 ||q-y||^2 + alpha/2 (q-qt)'(eta I - M'M)(q-qt)
# where qt is the previous iteration's q value
# equivalently, -sum_i (B_i*log((1-tau) q_i)+(1-B_i)*log(1-(1-tau) q_i)) + (alpha eta + beta)/2 * ||q-w||_2^2
# where w = - (M'(ut + alpha (M qt - xt)) + (vt - beta yt - alpha eta qt))/(alpha eta + beta)
  
	w = - ( t(M)%*%(u + alpha*(M%*%q - x)) + (v - beta*y - alpha*eta*q) )/(alpha*eta + beta)
	
	q[B==1] = (w[which(B==1)]+sqrt(w[which(B==1)]^2+4/(alpha*eta + beta)))/2
	q[B==0] = ((w[which(B==0)]+1/(1-tau))-sqrt((w[which(B==0)]-1/(1-tau))^2+4/(alpha*eta+beta)))/2
	q[q<eps] = eps
	q[q>1] = 1
	q
}

x_update = function(B, M, tau,eps,q,x,y,u,v,alpha,gamma,beta, eta, projection){
	# Proj_Mset (M q + u/alpha)
	x = projection(M%*%q + u/alpha) 
}

y_update = function(B, M, tau,eps,q,x,y,u,v,alpha,gamma,beta, eta){
	# Prof_B (q + v/beta)
	# where B = {sum_i B[i]/y[i]<= gamma}
	y = q + v/beta
	y[which(B==1)] = inverse_sum_prox((q+v/beta)[which(B==1)], gamma)
	y
}

u_update = function(B, M, tau,eps,q,x,y,u,v,alpha,gamma,beta, eta){
	u = u + alpha * (M%*%q -x)
	u
}

v_update = function(B, M, tau,eps,q,x,y,u,v,alpha,gamma,beta, eta){
	v = v + beta * (q-y)
	v
}

BH_method = function(pvals,alpha){
	khat=max(c(0,which(sort(pvals)<=alpha*(1:length(pvals))/length(pvals))))
	which(pvals<=alpha*khat/length(pvals))
}

Storey_method = function(pvals,alpha,thr){
	est_proportion_nulls=min(1,mean(pvals>thr)/(1-thr))
	pvals[pvals>thr] = Inf
	khat=max(c(0,which(sort(pvals)<=alpha/est_proportion_nulls*(1:length(pvals))/length(pvals))))
	which(pvals<=alpha/est_proportion_nulls*khat/length(pvals))
}
	
SABHA_method = function(pvals, qhat, alpha, tau){
	# Use the original, or estimated q as input
	pvals[pvals>tau] = Inf
	khat=max(c(0,which(sort(qhat*pvals)<=alpha*(1:length(pvals))/length(pvals))))
    which(qhat*pvals<=alpha*khat/length(pvals))
}



# COMMAND ----------

install.packages("vegan")
install.packages("ape")
install.packages("arules")
install.packages("jmuOutlier")
install.packages("coin")
install.packages("data.table")
install.packages("perm")
install.packages("ThresholdROC")
library(SparkR)
library(ape)
library(vegan)
library(arules)
library(jmuOutlier )
library(coin)
library(perm)
library(data.table)
library(ThresholdROC)



#install.packages("ggplot2")
 


# COMMAND ----------

install.packages("tibble")
install.packages("purrr")
library(purrr)
library(dplyr)
library(tibble)

# COMMAND ----------

# MAGIC %run /PhdProject/utils

# COMMAND ----------

# MAGIC %md
# MAGIC #uploading data

# COMMAND ----------

sc <- sparklyr::spark_connect(method = "databricks")
SparkR::registerTempTable(tableToDF("imagingFrame"), "imagingFrame")
imagingFrame<-     as_tibble ( as.data.frame(dplyr::tbl(sc, "imagingFrame")) )

SparkR::registerTempTable(tableToDF("numbsFrame"), "numbsFrame")
controlVsStudyFrame<-   as_tibble(as.data.frame(dplyr::tbl(sc, "numbsFrame")))



# COMMAND ----------

# MAGIC %md
# MAGIC #Utility functions

# COMMAND ----------

# performing the Permanova - method detecting weather there is a relation between some value and group of other values
# We will also check First the beta dispersion  to say weather it is sufficiently small in order to be able to still in a valid way perworm permanova
# @param mainFrame {DataFrame} representin all data we are analyzing
# @param referenceColumnName {String} name of column to which we want to check weather it has a significant correlation with all other columns (so reference column holds dependent variable)
# returns {List} return p value of permanova and p value related of beta dispersion in order for the test to be valid we need 
myPermanova <- function (mainFrame,referenceColumnName) {
reducedFr <-  mainFrame %>% dplyr::select(-all_of(referenceColumnName)) # %>% as.matrix() %>% sqrt() # square root transformation in order to reduce the effect of strongest values

  parmaNov<-adonis(reducedFr ~ mainFrame[[referenceColumnName]], perm=999)
  #calculating permanova p value 
  permanovaPValue <- as.data.frame(as.data.frame(parmaNov$aov.tab)[6])[1,1]

  dist<-vegdist(reducedFr, method='jaccard')
  dispersion<-betadisper(dist, group=mainFrame[[referenceColumnName]])
  
  c(permanovaPValue, permutest(dispersion) )
  
  
}

# COMMAND ----------

# based on the implementation from perm test https://cran.r-project.org/web/packages/perm/perm.pdf
myPermTest <- function(booleansColName, numbVectorColName, frame) {
  framee <-  frame %>% dplyr::select(booleansColName,numbVectorColName)  %>% dplyr::filter( !is.na(.[1]))%>% dplyr::filter( !is.na(.[2]))   
  numbVector <- framee[[numbVectorColName]]
  booleans<- framee[[booleansColName]]
  
trueOnes <-numbVector[booleans]
falseOnes <-numbVector[!booleans]
if(length(falseOnes)>1){
permTS(trueOnes , falseOnes)$p.values[1][["p.twosided"]]   } else{2}

}
#myPermTest("FocalAccumulation", "SuvInFocus", imagingFrame)


#Implementation of structure aware BH we supply p values and labels both vectors should be of the same length
myBH <- function (pValues,labels) {
n = length(pValues) # here important  we need to 
BH_Res = Storey_Res = SABHA_Res = rep(0,n)

############## set of parameters copied from fMRI example
alpha = 0.05 # target FDR level
tau = 0.5; eps = 0.1 # parameters for SABHA
ADMM_params = c(10^2, 10^3, 2, 15000, 1e-3) # alpha_ADMM,beta,eta,max_iters,converge_thr

# gather results
labels[SABHA_Res ==1]


qhat = Solve_q_block(pValues,tau,eps,labels,ADMM_params)

# it returns a vector with the same order as supplied at the beginning in this vecto when we did not achieved significance we get 0 when we did we get 1
SABHA_Res[SABHA_method(pValues,qhat,alpha,tau)] = 1
#selecting from labels those that are significantly significant 

labels[SABHA_Res ==1]
}


# COMMAND ----------

# MAGIC %md
# MAGIC #Analyzing image characteristics

# COMMAND ----------

#Here we define boolean columns we are intrested in 
colsprim<- names(imagingFrame)
cols<- colsprim[!colsprim %in% c("SuvInFocus","TBR" ,"Fluid", "IrregularBorders","FocalAccumulation" )]
pValsA<- cols %>%
  map( ~myPermTest(.x,  "SuvInFocus", imagingFrame ))
pValsB<- cols %>%
  map( ~myPermTest(.x,  "TBR", imagingFrame ))
df<- as.data.frame(cbind(cols, flatten_dbl(pValsA), flatten_dbl(pValsB) ))
colnames(df) <- c("imageCharacteristic", "SuvInFocus", "TBR")
#result<- frameWithPs %>% dplyr::filter(SuvInFocus<2) 
# result <-dplyr::transform(frameWithPs, SuvInFocus      = as.numeric(SuvInFocus), 
#                                     TBR = as.numeric(TBR))
df[] <- lapply(df, as.character)
df <- data.frame(lapply(df, function(x) ifelse(!is.na(as.numeric(x)), as.numeric(x),  x)))
result<- df


# COMMAND ----------

labels<- c( result[[1]],result[[1]]  ) 
pValues<- c(result[[2]],result[[3]])

n = length(pValues) # here important  we need to 
BH_Res = Storey_Res = SABHA_Res = rep(0,n)

############## set of parameters copied from fMRI example
alpha = 0.05 # target FDR level
tau = 0.5; eps = 0.1 # parameters for SABHA
ADMM_params = c(10^2, 10^3, 2, 15000, 1e-3) # alpha_ADMM,beta,eta,max_iters,converge_thr

# gather results
labels[SABHA_Res ==1]


qhat = Solve_q_block(pValues,tau,eps,labels,ADMM_params)

# it returns a vector with the same order as supplied at the beginning in this vecto when we did not achieved significance we get 0 when we did we get 1
SABHA_Res[SABHA_method(pValues,qhat,alpha,tau)] = 1
#selecting from labels those that are significantly significant 

result[[1]][SABHA_Res ==1] 

# COMMAND ----------

labels<- c( result[[1]],result[[1]]  ) 
pValues<- c(result[[2]],result[[3]])

validImagingTests<-  as.character(result[[1]][SABHA_Res ==1] )


# COMMAND ----------

# MAGIC %md
# MAGIC #looking for association pairs in imaging characteristics

# COMMAND ----------


frameOfBools<- imagingFrame %>% dplyr::select(-SuvInFocus, -TBR, -Fluid)


# COMMAND ----------

#rules <- apriori(frameOfBools, parameter = list(support = 0.01, confidence = 0.6))
trans <- as(frameOfBools, "transactions")
summary(trans)


# COMMAND ----------

install.packages("arulesViz")
library(arulesViz)
itemsets <- apriori(trans, parameter = list(target = "frequent",
  supp=0.001, minlen = 2, maxlen=4))

plot(head(sort(itemsets), n=50), method = "graph", control=list(cex=.8))



# COMMAND ----------

rules<- apriori(trans,parameter=list(supp=0.3,conf=.80, minlen=3,maxlen=7, target='rules')) # run a priori algorithms
arules<-rules

# COMMAND ----------

inspect(sort(arules, by="confidence", decreasing=TRUE))

# COMMAND ----------

rules_dataframe <- as(rules, 'data.frame') %>% filter(lift>1.2)



# COMMAND ----------

display(as.DataFrame(rules_dataframe))

# COMMAND ----------

# MAGIC %md
# MAGIC #Analyzing numerical values like Age ...

# COMMAND ----------

controlVsStudyFrame <-transform(controlVsStudyFrame, isStudy  = as.logical(isStudy), ageInYearsWhenSurgery = as.numeric(ageInYearsWhenSurgery)   )
controlVsStudyFrame

# COMMAND ----------

# "Comapring age during study in control and study group"
ageTest<- myPermTest("isStudy",  "ageInYearsWhenSurgery", controlVsStudyFrame )
# "Comapring prosthesis type in control and study group"
prosthesisTypeTest<-fisher.test(controlVsStudyFrame$prosthesisType == "stentgraft", controlVsStudyFrame$isStudy )$p.value
# "Comapring prosthesis location in control and study group"
locFrame <- controlVsStudyFrame %>% dplyr::filter(simplifiedClassification == "Y" | simplifiedClassification == "B") # filtering out or localistaions other than thoracic and abdominal aorta
locTest <- fisher.test(locFrame$simplifiedClassification == "Y", locFrame$isStudy )$p.value

studyVsControl<- myBH( c(ageTest, prosthesisTypeTest, locTest), c("ageTest","prosthesisTypeTest","locTest"))


# COMMAND ----------

# MAGIC %md
# MAGIC # looking for treshold values

# COMMAND ----------

#SUV max analysis
trueOnesSUV <-controlVsStudyFrame$SuvInFocus[controlVsStudyFrame$isStudy]
falseOnesSUV <-controlVsStudyFrame$SuvInFocus[!controlVsStudyFrame$isStudy]
#TBR analysis
trueOnesTBR <-controlVsStudyFrame$TBR[controlVsStudyFrame$isStudy]
falseOnesTBR <-controlVsStudyFrame$TBR[!controlVsStudyFrame$isStudy]

tresholdSuv<- thres2(trueOnesSUV,falseOnesSUV,0.01 )[[1]]$thres
tresholdTBR<- thres2(trueOnesTBR,falseOnesTBR,0.01 )[[1]]$thres


# COMMAND ----------

# MAGIC %md
# MAGIC #export data

# COMMAND ----------

sparkR.session()
#dataFrame p values related to imaging characteristics
result
# list of strings representing which imaging characteristics still have valid p after B-H correction
validImagingTests


# tests that are valid after BH correction
# ageTest string  "Comapring age during study in control and study group"
# prosthesisTypeTeststring "Comapring prosthesis type in control and study group"
# locTest string "Comapring prosthesis location in control and study group"
studyVsControl




# COMMAND ----------

#registering table so later with already developed functions in scala we can save them
registerTempTable(as.DataFrame(result), "imegeCharRawData")
registerTempTable(as.DataFrame(as.data.frame(cbind(validImagingTests,studyVsControl))), "statisticalValid")

titless<-c("ageTest","prosthesisTypeTest","locTest")
numbss<-c(ageTest, prosthesisTypeTest, locTest)
registerTempTable(as.DataFrame(as.data.frame(cbind(titless ,numbss))), "rawNotImage")

registerTempTable(as.DataFrame(as.data.frame(cbind(tresholdSuv ,tresholdTBR))), "tresholds")
registerTempTable(as.DataFrame(rules_dataframe),"arules")

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC createTablesWithMeta( "imegeCharRawData", "raw p values that we analyzed about the image characteristics" , spark.table("imegeCharRawData"))
# MAGIC createTablesWithMeta( "statisticalValid", "statistically significal dependence proven" , spark.table("statisticalValid"))
# MAGIC createTablesWithMeta( "rawNotImage", "p values related to test not connected immidiately to image" , spark.table("rawNotImage"))
# MAGIC createTablesWithMeta( "tresholds", "tresholds fur SUV max and TBR based on data in study and control groups" , spark.table("tresholds"))

# COMMAND ----------

# MAGIC %scala 
# MAGIC createTablesWithMeta( "arules", "associative rules" , spark.table("arules"))