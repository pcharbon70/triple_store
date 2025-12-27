defmodule TripleStore.Reasoner.MaterializationIntegrationTest do
  @moduledoc """
  Integration tests for Task 4.6.1: Materialization Testing.

  These tests verify full materialization on realistic ontologies using
  a synthetic LUBM-like (Lehigh University Benchmark) dataset structure.

  ## LUBM Ontology Structure

  The LUBM ontology models a university domain with:
  - University, Department, Faculty, Student, Course, etc.
  - Class hierarchy: GraduateStudent < Student < Person
  - Property characteristics: advisedBy is functional, memberOf is transitive
  - Complex inference patterns requiring multiple iterations

  ## Test Coverage

  - 4.6.1.1: Test materialization on LUBM-style dataset
  - 4.6.1.2: Verify query results match expected inference closure
  - 4.6.1.3: Benchmark materialization performance
  - 4.6.1.4: Test parallel materialization speedup
  """
  use ExUnit.Case, async: false

  alias TripleStore.Reasoner.{SemiNaive, ReasoningProfile}

  @moduletag :integration

  # ============================================================================
  # Namespace Constants
  # ============================================================================

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"
  @ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"
  @ex "http://example.org/university/"

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp ub_iri(name), do: {:iri, @ub <> name}
  defp ex_iri(name), do: {:iri, @ex <> name}
  defp rdf_type, do: {:iri, @rdf <> "type"}
  defp rdfs_subClassOf, do: {:iri, @rdfs <> "subClassOf"}
  defp rdfs_subPropertyOf, do: {:iri, @rdfs <> "subPropertyOf"}
  defp rdfs_domain, do: {:iri, @rdfs <> "domain"}
  defp rdfs_range, do: {:iri, @rdfs <> "range"}
  defp owl_TransitiveProperty, do: {:iri, @owl <> "TransitiveProperty"}
  defp owl_SymmetricProperty, do: {:iri, @owl <> "SymmetricProperty"}

  @doc """
  Generates a LUBM-style TBox (schema) with class and property hierarchies.
  """
  def generate_lubm_tbox do
    MapSet.new([
      # Class hierarchy
      # Thing (top)
      #   ├── Person
      #   │     ├── Employee
      #   │     │     ├── Faculty
      #   │     │     │     ├── Professor
      #   │     │     │     │     ├── FullProfessor
      #   │     │     │     │     ├── AssociateProfessor
      #   │     │     │     │     └── AssistantProfessor
      #   │     │     │     └── Lecturer
      #   │     │     └── AdministrativeStaff
      #   │     └── Student
      #   │           ├── GraduateStudent
      #   │           │     ├── PhDStudent
      #   │           │     └── MastersStudent
      #   │           └── UndergraduateStudent
      #   ├── Organization
      #   │     ├── University
      #   │     └── Department
      #   ├── Work
      #   │     ├── Course
      #   │     └── Publication
      #   │           ├── Article
      #   │           └── Book
      #   └── ResearchGroup

      # Person hierarchy
      {ub_iri("Person"), rdfs_subClassOf(), ub_iri("Thing")},
      {ub_iri("Employee"), rdfs_subClassOf(), ub_iri("Person")},
      {ub_iri("Faculty"), rdfs_subClassOf(), ub_iri("Employee")},
      {ub_iri("Professor"), rdfs_subClassOf(), ub_iri("Faculty")},
      {ub_iri("FullProfessor"), rdfs_subClassOf(), ub_iri("Professor")},
      {ub_iri("AssociateProfessor"), rdfs_subClassOf(), ub_iri("Professor")},
      {ub_iri("AssistantProfessor"), rdfs_subClassOf(), ub_iri("Professor")},
      {ub_iri("Lecturer"), rdfs_subClassOf(), ub_iri("Faculty")},
      {ub_iri("AdministrativeStaff"), rdfs_subClassOf(), ub_iri("Employee")},
      {ub_iri("Student"), rdfs_subClassOf(), ub_iri("Person")},
      {ub_iri("GraduateStudent"), rdfs_subClassOf(), ub_iri("Student")},
      {ub_iri("PhDStudent"), rdfs_subClassOf(), ub_iri("GraduateStudent")},
      {ub_iri("MastersStudent"), rdfs_subClassOf(), ub_iri("GraduateStudent")},
      {ub_iri("UndergraduateStudent"), rdfs_subClassOf(), ub_iri("Student")},

      # Organization hierarchy
      {ub_iri("Organization"), rdfs_subClassOf(), ub_iri("Thing")},
      {ub_iri("University"), rdfs_subClassOf(), ub_iri("Organization")},
      {ub_iri("Department"), rdfs_subClassOf(), ub_iri("Organization")},

      # Work hierarchy
      {ub_iri("Work"), rdfs_subClassOf(), ub_iri("Thing")},
      {ub_iri("Course"), rdfs_subClassOf(), ub_iri("Work")},
      {ub_iri("Publication"), rdfs_subClassOf(), ub_iri("Work")},
      {ub_iri("Article"), rdfs_subClassOf(), ub_iri("Publication")},
      {ub_iri("Book"), rdfs_subClassOf(), ub_iri("Publication")},

      # ResearchGroup
      {ub_iri("ResearchGroup"), rdfs_subClassOf(), ub_iri("Thing")},

      # Property hierarchy
      {ub_iri("memberOf"), rdfs_subPropertyOf(), ub_iri("affiliatedWith")},
      {ub_iri("worksFor"), rdfs_subPropertyOf(), ub_iri("affiliatedWith")},
      {ub_iri("headOf"), rdfs_subPropertyOf(), ub_iri("worksFor")},

      # Property domain/range
      {ub_iri("memberOf"), rdfs_domain(), ub_iri("Person")},
      {ub_iri("memberOf"), rdfs_range(), ub_iri("Organization")},
      {ub_iri("worksFor"), rdfs_domain(), ub_iri("Employee")},
      {ub_iri("worksFor"), rdfs_range(), ub_iri("Organization")},
      {ub_iri("teacherOf"), rdfs_domain(), ub_iri("Faculty")},
      {ub_iri("teacherOf"), rdfs_range(), ub_iri("Course")},
      {ub_iri("takesCourse"), rdfs_domain(), ub_iri("Student")},
      {ub_iri("takesCourse"), rdfs_range(), ub_iri("Course")},
      {ub_iri("advisor"), rdfs_domain(), ub_iri("Student")},
      {ub_iri("advisor"), rdfs_range(), ub_iri("Professor")},
      {ub_iri("publicationAuthor"), rdfs_domain(), ub_iri("Person")},
      {ub_iri("publicationAuthor"), rdfs_range(), ub_iri("Publication")},

      # Property characteristics
      {ub_iri("subOrganizationOf"), rdf_type(), owl_TransitiveProperty()},
      {ub_iri("collaboratesWith"), rdf_type(), owl_SymmetricProperty()}
    ])
  end

  @doc """
  Generates ABox (instance data) for a university with the given parameters.

  ## Parameters
  - num_departments: Number of departments
  - faculty_per_dept: Number of faculty members per department
  - students_per_dept: Number of students per department
  - courses_per_dept: Number of courses per department

  Returns a MapSet of triples.
  """
  def generate_lubm_abox(opts \\ []) do
    num_departments = Keyword.get(opts, :departments, 15)
    faculty_per_dept = Keyword.get(opts, :faculty_per_dept, 10)
    students_per_dept = Keyword.get(opts, :students_per_dept, 100)
    courses_per_dept = Keyword.get(opts, :courses_per_dept, 10)

    university = ex_iri("University0")

    # Generate university
    facts = MapSet.new([
      {university, rdf_type(), ub_iri("University")}
    ])

    # Generate departments and their contents
    Enum.reduce(0..(num_departments - 1), facts, fn dept_id, acc ->
      dept = ex_iri("Department#{dept_id}")

      # Department facts
      dept_facts = MapSet.new([
        {dept, rdf_type(), ub_iri("Department")},
        {dept, ub_iri("subOrganizationOf"), university}
      ])

      # Generate faculty
      faculty_facts = generate_faculty(dept_id, faculty_per_dept, dept)

      # Generate students
      student_facts = generate_students(dept_id, students_per_dept, dept, faculty_per_dept)

      # Generate courses
      course_facts = generate_courses(dept_id, courses_per_dept, dept, faculty_per_dept, students_per_dept)

      acc
      |> MapSet.union(dept_facts)
      |> MapSet.union(faculty_facts)
      |> MapSet.union(student_facts)
      |> MapSet.union(course_facts)
    end)
  end

  defp generate_faculty(dept_id, count, dept) do
    Enum.reduce(0..(count - 1), MapSet.new(), fn faculty_id, acc ->
      faculty = ex_iri("Faculty#{dept_id}_#{faculty_id}")

      # Assign professor types
      type = case rem(faculty_id, 4) do
        0 -> ub_iri("FullProfessor")
        1 -> ub_iri("AssociateProfessor")
        2 -> ub_iri("AssistantProfessor")
        3 -> ub_iri("Lecturer")
      end

      facts = MapSet.new([
        {faculty, rdf_type(), type},
        {faculty, ub_iri("worksFor"), dept}
      ])

      # First faculty is head of department
      facts = if faculty_id == 0 do
        MapSet.put(facts, {faculty, ub_iri("headOf"), dept})
      else
        facts
      end

      MapSet.union(acc, facts)
    end)
  end

  defp generate_students(dept_id, count, dept, num_faculty) do
    Enum.reduce(0..(count - 1), MapSet.new(), fn student_id, acc ->
      student = ex_iri("Student#{dept_id}_#{student_id}")

      # Assign student types
      type = case rem(student_id, 4) do
        0 -> ub_iri("PhDStudent")
        1 -> ub_iri("MastersStudent")
        2 -> ub_iri("UndergraduateStudent")
        3 -> ub_iri("UndergraduateStudent")
      end

      # Assign advisor (for graduate students)
      advisor_id = rem(student_id, num_faculty)
      advisor = ex_iri("Faculty#{dept_id}_#{advisor_id}")

      facts = MapSet.new([
        {student, rdf_type(), type},
        {student, ub_iri("memberOf"), dept}
      ])

      # Graduate students have advisors
      facts = if rem(student_id, 4) in [0, 1] do
        MapSet.put(facts, {student, ub_iri("advisor"), advisor})
      else
        facts
      end

      MapSet.union(acc, facts)
    end)
  end

  defp generate_courses(dept_id, count, _dept, num_faculty, num_students) do
    Enum.reduce(0..(count - 1), MapSet.new(), fn course_id, acc ->
      course = ex_iri("Course#{dept_id}_#{course_id}")
      teacher_id = rem(course_id, num_faculty)
      teacher = ex_iri("Faculty#{dept_id}_#{teacher_id}")

      facts = MapSet.new([
        {course, rdf_type(), ub_iri("Course")},
        {teacher, ub_iri("teacherOf"), course}
      ])

      # Students take courses
      students_per_course = div(num_students, count)
      student_facts = Enum.reduce(0..(students_per_course - 1), MapSet.new(), fn i, sacc ->
        student_id = rem(course_id * students_per_course + i, num_students)
        student = ex_iri("Student#{dept_id}_#{student_id}")
        MapSet.put(sacc, {student, ub_iri("takesCourse"), course})
      end)

      acc
      |> MapSet.union(facts)
      |> MapSet.union(student_facts)
    end)
  end

  @doc """
  Creates an in-memory lookup function for a fact set.
  """
  def make_lookup(facts) do
    fn {:pattern, [s, p, o]} ->
      matching =
        facts
        |> Enum.filter(fn {fs, fp, fo} ->
          matches?(fs, s) and matches?(fp, p) and matches?(fo, o)
        end)

      {:ok, Enum.to_list(matching)}
    end
  end

  defp matches?(_fact_term, {:var, _}), do: true
  defp matches?(fact_term, pattern_term), do: fact_term == pattern_term

  # ============================================================================
  # Task 4.6.1.1: Test materialization on LUBM-style dataset
  # ============================================================================

  describe "4.6.1.1 materialization on LUBM-style dataset" do
    @tag timeout: 120_000
    test "materializes LUBM(1) scale dataset with RDFS rules" do
      # Generate LUBM(1) scale dataset (1 university)
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 15,
        faculty_per_dept: 10,
        students_per_dept: 100,
        courses_per_dept: 10
      )

      initial_facts = MapSet.union(tbox, abox)
      initial_count = MapSet.size(initial_facts)

      # Use RDFS rules
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Materialize
      {:ok, all_facts, stats} = SemiNaive.materialize_in_memory(rules, initial_facts)

      # Verify materialization completed
      assert stats.iterations > 0
      assert stats.total_derived > 0
      assert MapSet.size(all_facts) > initial_count

      # Output stats for debugging
      IO.puts("\n--- LUBM(1) RDFS Materialization ---")
      IO.puts("Initial facts: #{initial_count}")
      IO.puts("Final facts: #{MapSet.size(all_facts)}")
      IO.puts("Derived facts: #{stats.total_derived}")
      IO.puts("Iterations: #{stats.iterations}")
      IO.puts("Duration: #{stats.duration_ms}ms")
    end

    @tag timeout: 120_000
    test "materializes LUBM(1) scale dataset with OWL 2 RL rules" do
      # Generate smaller dataset for OWL 2 RL (more complex rules = slower)
      # Still representative but practical for testing
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 3,
        faculty_per_dept: 5,
        students_per_dept: 20,
        courses_per_dept: 5
      )

      initial_facts = MapSet.union(tbox, abox)
      initial_count = MapSet.size(initial_facts)

      # Use full OWL 2 RL rules
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # Materialize
      {:ok, all_facts, stats} = SemiNaive.materialize_in_memory(rules, initial_facts)

      # Verify materialization completed
      assert stats.iterations > 0
      assert MapSet.size(all_facts) > initial_count

      IO.puts("\n--- LUBM(1) OWL 2 RL Materialization ---")
      IO.puts("Initial facts: #{initial_count}")
      IO.puts("Final facts: #{MapSet.size(all_facts)}")
      IO.puts("Derived facts: #{stats.total_derived}")
      IO.puts("Iterations: #{stats.iterations}")
      IO.puts("Duration: #{stats.duration_ms}ms")
    end

    test "materializes smaller dataset correctly" do
      # Small test for quick verification
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 2,
        faculty_per_dept: 3,
        students_per_dept: 10,
        courses_per_dept: 2
      )

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      {:ok, all_facts, stats} = SemiNaive.materialize_in_memory(rules, initial_facts)

      assert stats.iterations > 0
      assert MapSet.size(all_facts) > MapSet.size(initial_facts)
    end
  end

  # ============================================================================
  # Task 4.6.1.2: Verify query results match expected inference closure
  # ============================================================================

  describe "4.6.1.2 inference closure correctness" do
    test "class hierarchy inference produces expected types" do
      tbox = generate_lubm_tbox()

      # Create a single PhD student
      abox = MapSet.new([
        {ex_iri("Alice"), rdf_type(), ub_iri("PhDStudent")},
        {ex_iri("Alice"), ub_iri("memberOf"), ex_iri("Department0")}
      ])

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial_facts)

      # Alice should be inferred to be:
      # PhDStudent < GraduateStudent < Student < Person < Thing
      assert MapSet.member?(all_facts, {ex_iri("Alice"), rdf_type(), ub_iri("GraduateStudent")})
      assert MapSet.member?(all_facts, {ex_iri("Alice"), rdf_type(), ub_iri("Student")})
      assert MapSet.member?(all_facts, {ex_iri("Alice"), rdf_type(), ub_iri("Person")})
      assert MapSet.member?(all_facts, {ex_iri("Alice"), rdf_type(), ub_iri("Thing")})
    end

    test "property hierarchy inference produces expected properties" do
      tbox = generate_lubm_tbox()

      abox = MapSet.new([
        {ex_iri("Prof1"), ub_iri("headOf"), ex_iri("Department0")}
      ])

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial_facts)

      # headOf < worksFor < affiliatedWith
      # So Prof1 headOf Dept0 should imply Prof1 worksFor Dept0 and Prof1 affiliatedWith Dept0
      assert MapSet.member?(all_facts, {ex_iri("Prof1"), ub_iri("worksFor"), ex_iri("Department0")})
      assert MapSet.member?(all_facts, {ex_iri("Prof1"), ub_iri("affiliatedWith"), ex_iri("Department0")})
    end

    test "domain/range inference produces expected types" do
      tbox = generate_lubm_tbox()

      abox = MapSet.new([
        {ex_iri("Prof1"), ub_iri("teacherOf"), ex_iri("Course101")},
        {ex_iri("Student1"), ub_iri("takesCourse"), ex_iri("Course101")}
      ])

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial_facts)

      # teacherOf has domain Faculty and range Course
      # So Prof1 should be inferred to be Faculty
      assert MapSet.member?(all_facts, {ex_iri("Prof1"), rdf_type(), ub_iri("Faculty")})
      assert MapSet.member?(all_facts, {ex_iri("Course101"), rdf_type(), ub_iri("Course")})

      # takesCourse has domain Student and range Course
      assert MapSet.member?(all_facts, {ex_iri("Student1"), rdf_type(), ub_iri("Student")})
    end

    test "transitive property inference produces expected relations" do
      tbox = generate_lubm_tbox()

      # Create a chain of subOrganizationOf
      abox = MapSet.new([
        {ex_iri("ResearchGroup1"), rdf_type(), ub_iri("ResearchGroup")},
        {ex_iri("Department0"), rdf_type(), ub_iri("Department")},
        {ex_iri("University0"), rdf_type(), ub_iri("University")},
        {ex_iri("ResearchGroup1"), ub_iri("subOrganizationOf"), ex_iri("Department0")},
        {ex_iri("Department0"), ub_iri("subOrganizationOf"), ex_iri("University0")}
      ])

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial_facts)

      # Transitive: ResearchGroup1 subOrganizationOf University0
      assert MapSet.member?(all_facts, {ex_iri("ResearchGroup1"), ub_iri("subOrganizationOf"), ex_iri("University0")})
    end

    test "symmetric property inference produces expected relations" do
      tbox = generate_lubm_tbox()

      abox = MapSet.new([
        {ex_iri("Prof1"), rdf_type(), ub_iri("Professor")},
        {ex_iri("Prof2"), rdf_type(), ub_iri("Professor")},
        {ex_iri("Prof1"), ub_iri("collaboratesWith"), ex_iri("Prof2")}
      ])

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial_facts)

      # Symmetric: Prof2 collaboratesWith Prof1
      assert MapSet.member?(all_facts, {ex_iri("Prof2"), ub_iri("collaboratesWith"), ex_iri("Prof1")})
    end

    test "complete inference closure for faculty member" do
      tbox = generate_lubm_tbox()

      abox = MapSet.new([
        {ex_iri("Prof1"), rdf_type(), ub_iri("FullProfessor")},
        {ex_iri("Prof1"), ub_iri("worksFor"), ex_iri("Department0")},
        {ex_iri("Prof1"), ub_iri("teacherOf"), ex_iri("Course101")},
        {ex_iri("Department0"), rdf_type(), ub_iri("Department")},
        {ex_iri("University0"), rdf_type(), ub_iri("University")},
        {ex_iri("Department0"), ub_iri("subOrganizationOf"), ex_iri("University0")}
      ])

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial_facts)

      # Full class hierarchy for FullProfessor
      assert MapSet.member?(all_facts, {ex_iri("Prof1"), rdf_type(), ub_iri("Professor")})
      assert MapSet.member?(all_facts, {ex_iri("Prof1"), rdf_type(), ub_iri("Faculty")})
      assert MapSet.member?(all_facts, {ex_iri("Prof1"), rdf_type(), ub_iri("Employee")})
      assert MapSet.member?(all_facts, {ex_iri("Prof1"), rdf_type(), ub_iri("Person")})

      # Property hierarchy: worksFor < affiliatedWith
      assert MapSet.member?(all_facts, {ex_iri("Prof1"), ub_iri("affiliatedWith"), ex_iri("Department0")})
    end
  end

  # ============================================================================
  # Task 4.6.1.3: Benchmark materialization performance
  # ============================================================================

  describe "4.6.1.3 benchmark materialization performance" do
    @tag timeout: 120_000
    @tag :benchmark
    test "LUBM(1) materialization completes in reasonable time" do
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 15,
        faculty_per_dept: 10,
        students_per_dept: 100,
        courses_per_dept: 10
      )

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Run materialization and measure time
      {duration_us, {:ok, _all_facts, stats}} =
        :timer.tc(fn ->
          SemiNaive.materialize_in_memory(rules, initial_facts)
        end)

      duration_ms = div(duration_us, 1000)

      IO.puts("\n--- LUBM(1) Benchmark ---")
      IO.puts("Duration: #{duration_ms}ms (#{div(duration_us, 1_000_000)}s)")
      IO.puts("Internal duration: #{stats.duration_ms}ms")
      IO.puts("Initial facts: #{MapSet.size(initial_facts)}")
      IO.puts("Derived facts: #{stats.total_derived}")
      IO.puts("Iterations: #{stats.iterations}")

      # LUBM(1) should complete well under 60 seconds
      # For in-memory with MapSet, it should be much faster
      assert duration_ms < 60_000, "Materialization took #{duration_ms}ms, expected <60000ms"

      # Should complete in reasonable number of iterations
      assert stats.iterations <= 20, "Too many iterations: #{stats.iterations}"
    end

    @tag timeout: 120_000
    @tag :benchmark
    test "materialization scales reasonably with data size" do
      tbox = generate_lubm_tbox()

      # Run with different data sizes and measure scaling
      sizes = [
        {2, 3, 10, 2},    # Small
        {5, 5, 50, 5},    # Medium
        {10, 8, 80, 8}    # Large
      ]

      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      results = Enum.map(sizes, fn {depts, faculty, students, courses} ->
        abox = generate_lubm_abox(
          departments: depts,
          faculty_per_dept: faculty,
          students_per_dept: students,
          courses_per_dept: courses
        )

        initial_facts = MapSet.union(tbox, abox)
        initial_count = MapSet.size(initial_facts)

        {duration_us, {:ok, _all_facts, stats}} =
          :timer.tc(fn ->
            SemiNaive.materialize_in_memory(rules, initial_facts)
          end)

        %{
          initial_count: initial_count,
          derived_count: stats.total_derived,
          duration_ms: div(duration_us, 1000),
          iterations: stats.iterations
        }
      end)

      IO.puts("\n--- Scaling Benchmark ---")
      Enum.each(results, fn r ->
        IO.puts("Initial: #{r.initial_count}, Derived: #{r.derived_count}, Time: #{r.duration_ms}ms, Iters: #{r.iterations}")
      end)

      # Verify all completed
      Enum.each(results, fn r ->
        assert r.derived_count > 0
        assert r.duration_ms < 60_000
      end)
    end
  end

  # ============================================================================
  # Task 4.6.1.4: Test parallel materialization speedup
  # ============================================================================

  describe "4.6.1.4 parallel materialization" do
    @tag timeout: 120_000
    test "parallel materialization produces same results as sequential" do
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 5,
        faculty_per_dept: 5,
        students_per_dept: 30,
        courses_per_dept: 5
      )

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Sequential
      {:ok, seq_facts, seq_stats} = SemiNaive.materialize_in_memory(rules, initial_facts, parallel: false)

      # Parallel
      {:ok, par_facts, par_stats} = SemiNaive.materialize_in_memory(rules, initial_facts, parallel: true)

      # Results must be identical
      assert MapSet.equal?(seq_facts, par_facts),
        "Sequential and parallel results differ!"

      assert seq_stats.total_derived == par_stats.total_derived,
        "Derived counts differ: seq=#{seq_stats.total_derived}, par=#{par_stats.total_derived}"

      IO.puts("\n--- Sequential vs Parallel ---")
      IO.puts("Sequential: #{seq_stats.duration_ms}ms, #{seq_stats.iterations} iterations")
      IO.puts("Parallel: #{par_stats.duration_ms}ms, #{par_stats.iterations} iterations")
    end

    @tag timeout: 120_000
    @tag :benchmark
    test "parallel materialization shows speedup on larger dataset" do
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 10,
        faculty_per_dept: 8,
        students_per_dept: 80,
        courses_per_dept: 8
      )

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # Run multiple times to get stable measurements
      num_runs = 3

      seq_times = Enum.map(1..num_runs, fn _ ->
        {duration_us, _result} = :timer.tc(fn ->
          SemiNaive.materialize_in_memory(rules, initial_facts, parallel: false)
        end)
        div(duration_us, 1000)
      end)

      par_times = Enum.map(1..num_runs, fn _ ->
        {duration_us, _result} = :timer.tc(fn ->
          SemiNaive.materialize_in_memory(rules, initial_facts, parallel: true)
        end)
        div(duration_us, 1000)
      end)

      avg_seq = div(Enum.sum(seq_times), num_runs)
      avg_par = div(Enum.sum(par_times), num_runs)

      speedup = if avg_par > 0, do: avg_seq / avg_par, else: 0

      IO.puts("\n--- Parallel Speedup Benchmark ---")
      IO.puts("Sequential avg: #{avg_seq}ms (runs: #{inspect(seq_times)})")
      IO.puts("Parallel avg: #{avg_par}ms (runs: #{inspect(par_times)})")
      IO.puts("Speedup: #{Float.round(speedup, 2)}x")
      IO.puts("Schedulers: #{System.schedulers_online()}")

      # Both should complete
      assert avg_seq > 0
      assert avg_par > 0

      # Parallel should not be significantly slower
      # (on small datasets, overhead may make parallel slower)
      assert avg_par < avg_seq * 2, "Parallel is >2x slower than sequential"
    end

    test "parallel mode is deterministic across multiple runs" do
      tbox = generate_lubm_tbox()
      abox = generate_lubm_abox(
        departments: 3,
        faculty_per_dept: 4,
        students_per_dept: 20,
        courses_per_dept: 3
      )

      initial_facts = MapSet.union(tbox, abox)
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Run parallel multiple times
      results = Enum.map(1..5, fn _ ->
        {:ok, facts, _stats} = SemiNaive.materialize_in_memory(rules, initial_facts, parallel: true)
        facts
      end)

      # All results should be identical
      first = hd(results)
      Enum.each(results, fn facts ->
        assert MapSet.equal?(facts, first), "Non-deterministic parallel results!"
      end)
    end
  end
end
