/*
 * Authors: CS4500 Staff (modified to be compatbile with our code)
 */

// lang::Cpp

#include <cctype>
#include <string>
#include "../src/dataframe/dataframe.h"
#include "../src/dataframe/wrapper.h"
#include "../src/application.h"

/**************************************************************************
 * A bit set contains size() booleans that are initialize to false and can
 * be set to true with the set() method. The test() method returns the
 * value. Does not grow.
 ************************************************************************/
class Set
{
public:
  std::vector<bool> vals_;  // owned; data

  /** Creates a set of the same size as the dataframe. */
  Set(std::shared_ptr<DataFrame> df) {}

  /** Creates an empty set. */
  Set() = default;

  ~Set() = default;

  /** Add idx to the set. If idx is out of bound, ignore it.  Out of bound
   *  values can occur if there are references to pids or uids in commits
   *  that did not appear in projects or users.
   */
  void set(size_t idx)
  {
    if (idx >= vals_.size())
      return; // ignoring out of bound writes
    vals_[idx] = true;
  }

  /** Is idx in the set?  See comment for set(). */
  bool test(size_t idx)
  {
    if (idx >= vals_.size())
      return true; // ignoring out of bound reads
    return vals_[idx];
  }

  size_t size() { return vals_.size(); }

  /** Performs set union in place. */
  void union_(Set &from)
  {
    for (size_t i = 0; i < from.size(); i++)
      if (from.test(i))
        set(i);
  }
};


/*******************************************************************************
 * A SetUpdater is a reader that gets the first column of the data frame and
 * sets the corresponding value in the given set.
 ******************************************************************************/
class SetUpdater : public Reader
{
public:
  Set &set_; // set to update

  SetUpdater(Set &set) : set_(set) {}

  /** Assume a row with at least one column of type I. Assumes that there
   * are no missing. Reads the value and sets the corresponding position.
   * The return value is irrelevant here. */
  bool visit(Row &row)
  {
    set_.set(row.get_int(0));
    return false;
  }
};


/*****************************************************************************
 * A SetWriter copies all the values present in the set into a one-column
 * dataframe. The data contains all the values in the set. The dataframe has
 * at least one integer column.
 ****************************************************************************/
class SetWriter : public Writer
{
public:
  Set &set_;  // set to read from
  int i_ = 0; // position in set

  SetWriter(Set &set) : set_(set) {}

  /** Skip over false values and stop when the entire set has been seen */
  bool done()
  {
    while (i_ < set_.size() && set_.test(i_) == false)
      ++i_;
    return i_ == set_.size();
  }

  void visit(Row &row) {
    Int i(i_++);
    row.set(0, i);
  }
};




/***************************************************************************
 * The ProjectTagger is a reader that is mapped over commits, and marks all
 * of the projects to which a collaborator of Linus committed as an author.
 * The commit dataframe has the form:
 *    pid x uid x uid
 * where the pid is the identifier of a project and the uids are the
 * identifiers of the author and committer. If the author is a collaborator
 * of Linus, then the project is added to the set. If the project was
 * already tagged then it is not added to the set of newProjects.
 *************************************************************************/
class ProjectsTagger : public Reader
{
public:
  Set &uSet;       // set of collaborator
  Set &pSet;       // set of projects of collaborators
  Set newProjects; // newly tagged collaborator projects

  ProjectsTagger(Set &uSet, Set &pSet, std::shared_ptr<DataFrame> proj) : uSet(uSet), pSet(pSet), newProjects(proj) {}

  /** The data frame must have at least two integer columns. The newProject
   * set keeps track of projects that were newly tagged (they will have to
   * be communicated to other nodes). */
  bool visit(Row &row) override
  {
    int pid = row.get_int(0);
    int uid = row.get_int(1);
    if (uSet.test(uid))
      if (!pSet.test(pid))
      {
        pSet.set(pid);
        newProjects.set(pid);
      }
    return false;
  }
};

/***************************************************************************
 * The UserTagger is a reader that is mapped over commits, and marks all of
 * the users which commmitted to a project to which a collaborator of Linus
 * also committed as an author. The commit dataframe has the form:
 *    pid x uid x uid
 * where the pid is the idefntifier of a project and the uids are the
 * identifiers of the author and committer. 
 *************************************************************************/
class UsersTagger : public Reader
{
public:
  Set &pSet;
  Set &uSet;
  Set newUsers;

  UsersTagger(Set &pSet, Set &uSet, std::shared_ptr<DataFrame> users) : pSet(pSet), uSet(uSet), newUsers() {}

  bool visit(Row &row) override
  {
    int pid = row.get_int(0);
    int uid = row.get_int(1);
    if (pSet.test(pid))
      if (!uSet.test(uid))
      {
        uSet.set(uid);
        newUsers.set(uid);
      }
    return false;
  }
};

/**
 * The input data is a processed extract from GitHub.
 *
 * projects:  I x S   --  The first field is a project id (or pid).
 *                    --  The second field is that project's name.
 *                    --  In a well-formed dataset the largest pid
 *                    --  is equal to the number of projects.
 *
 * users:    I x S    -- The first field is a user id, (or uid).
 *                    -- The second field is that user's name.
 *
 * commits: I x I x I -- The fields are pid, uid, uid', each row represent
 *                    -- a commit to project pid, written by user uid
 *                    -- and committed by user uid',
 **/

/*************************************************************************
 * This computes the collaborators of Linus Torvalds.
 * is the linus example using the adapter.  And slightly revised
 *   algorithm that only ever trades the deltas.
 **************************************************************************/
class Linus : public Application
{
public:
  int DEGREES = 4;  // How many degrees of separation form linus?
  int LINUS = 4967; // The uid of Linus (offset in the user df)
  std::string PROJ = "datasets/projects.ltgt";
  std::string USER = "datasets/users.ltgt";
  std::string COMM = "datasets/commits.ltgt";
  std::shared_ptr<DataFrame> projects; //  pid x project name
  std::shared_ptr<DataFrame> users;    // uid x user name
  std::shared_ptr<DataFrame> commits;  // pid x uid x uid
  std::shared_ptr<Set> uSet;           // Linus' collaborators
  std::shared_ptr<Set> pSet;           // projects of collaborators
  size_t num_nodes_ = 0;

  Linus(size_t idx, std::shared_ptr<NetworkIfc> net, size_t num_nodes)
      : Application(idx, net, num_nodes), num_nodes_(num_nodes) {}

  /** Compute DEGREES of Linus.  */
  void run_() override
  {
    readInput();
    for (size_t i = 0; i < DEGREES; i++)
      step(i);
  }

  /** Node 0 reads three files, cointainng projects, users and commits, and
   *  creates thre dataframes. All other nodes wait and load the three
   *  dataframes. Once we know the size of users and projects, we create
   *  sets of each (uSet and pSet). We also output a data frame with a the
   *  'tagged' users. At this point the dataframe consists of only
   *  Linus. **/
  void readInput()
  {
    auto pK = std::make_shared<Key>("projs", idx_);
    auto uK = std::make_shared<Key>("usrs", idx_);
    auto cK = std::make_shared<Key>("comts", idx_);
    auto linusKey = std::make_shared<Key>("users-0-0", idx_);
    if (idx_ == 0)
    {
      std::cout << "Reading..." << std::endl;
      projects = DataFrame::fromFile(PROJ, pK, kv);
      std::cout << "    " << projects->nrows() << " projects" << std::endl;
      ;
      users = DataFrame::fromFile(USER, uK, kv);
      std::cout << "    " << users->nrows() << " users" << std::endl;
      ;
      commits = DataFrame::fromFile(COMM, cK, kv);
      std::cout << "    " << commits->nrows() << " commits" << std::endl;
      // This dataframe contains the id of Linus.
      DataFrame::fromScalarInt(linusKey, kv, LINUS);
    }
    else
    {
      auto projects_value = kv->waitAndGet(*pK);
      Deserializer projects_dser(projects_value.data(), projects_value.length());
      projects = DataFrame::deserialize(projects_dser);

      auto users_value = kv->waitAndGet(*uK);
      Deserializer users_dser(users_value.data(), users_value.length());
      users = DataFrame::deserialize(users_dser);

      auto commits_value = kv->waitAndGet(*cK);
      Deserializer commits_dser(commits_value.data(), commits_value.length());
      commits = DataFrame::deserialize(commits_dser);
    }
    uSet = std::make_shared<Set>(users);
    pSet = std::make_shared<Set>(projects);
  }

  /** Performs a step of the linus calculation. It operates over the three
  *  datafrrames (projects, users, commits), the sets of tagged users and
  *  projects, and the users added in the previous round. */
  void step(int stage)
  {
    std::cout << "Stage " << stage << std::endl;
    // Key of the shape: users-stage-0
    std::string uK_name = "users-";
    uK_name += std::to_string(stage);
    uK_name += "-0";
    auto uK = std::make_shared<Key>(uK_name, idx_);

    // A df with all the users added on the previous round
    Value df_val = kv->waitAndGet(*uK);
    Deserializer dser(df_val.data(), df_val.length());
    auto newUsers = DataFrame::deserialize(dser);
    Set delta(users);
    SetUpdater upd(delta);
    newUsers->map(upd); // all of the new users are copied to delta.
    ProjectsTagger ptagger(delta, *pSet, projects);
    commits->local_map(ptagger, kv); // marking all projects touched by delta
    merge(ptagger.newProjects, "projects-", stage);
    pSet->union_(ptagger.newProjects); //
    UsersTagger utagger(ptagger.newProjects, *uSet, users);
    commits->local_map(utagger, kv);
    merge(utagger.newUsers, "users-", stage + 1);
    uSet->union_(utagger.newUsers);
    std::cout << "    after stage " << stage << ":" << std::endl;
    std::cout << "        tagged projects: " << pSet->size() << std::endl;
    ;
    std::cout << "        tagged users: " << uSet->size() << std::endl;
    ;
  }

  /** Gather updates to the given set from all the nodes in the systems.
   * The union of those updates is then published as dataframe.  The key
   * used for the otuput is of the form "name-stage-0" where name is either
   * 'users' or 'projects', stage is the degree of separation being
   * computed.
   */
  void merge(Set &set, std::string name, int stage)
  {
    if (this_node() == 0)
    {
      for (size_t i = 1; i < num_nodes_; ++i)
      {
        std::string nK_name = name;
        nK_name += std::to_string(stage);
        nK_name += "-";
        nK_name += std::to_string(i);
        auto nK = std::make_shared<Key>(nK_name, idx_);
        Value delta_val = kv->waitAndGet(*nK);
        Deserializer dser(delta_val.data(), delta_val.length());
        auto delta = DataFrame::deserialize(dser);
        std::cout << "    received delta of " << delta->nrows() << " elements from node " << i << std::endl;;
        SetUpdater upd(set);
        delta->map(upd);
      }
      std::cout << "    storing " << set.size() << " merged elements" << std::endl;;
      SetWriter writer(set);

      std::string k_name = name;
      k_name += std::to_string(stage);
      k_name += "-0";
      auto k = std::make_shared<Key>(k_name, idx_);
      DataFrame::fromVisitor(k, kv, "I", writer);
    }
    else
    {
      std::cout << "    sending " << set.size() << " elements to master node" << std::endl;;
      SetWriter writer(set);
      std::string k_name = name;
      k_name += std::to_string(stage);
      k_name += "-";
      k_name += std::to_string(idx_);
      auto k = std::make_shared<Key>(k_name, idx_);
      DataFrame::fromVisitor(k, kv, "I", writer);
      std::string mK_name = name;
      mK_name += std::to_string(stage);
      mK_name += "-0";
      auto mK = std::make_shared<Key>(mK_name, idx_);

      Value merged_val = kv->waitAndGet(*mK);
      Deserializer dser(merged_val.data(), merged_val.length());
      auto merged = DataFrame::deserialize(dser);
      std::cout << "    receiving " << merged->nrows() << " merged elements" << std::endl;;
      SetUpdater upd(set);
      merged->map(upd);
    }
  }
}; // Linus

/**
 * Runs the Milestone 5 Demo.
 */
int main()
{
  int num_nodes = 1;
  auto net = std::make_shared<NetworkPseudo>(num_nodes);
  Linus linus(0, net, num_nodes);
  //linus.run_();
  std::cout << "linus not yet implemented... sorry" << std::endl;
  return 0;
}